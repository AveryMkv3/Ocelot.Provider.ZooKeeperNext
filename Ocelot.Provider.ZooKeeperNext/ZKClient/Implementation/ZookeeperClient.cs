using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ocelot.Provider.ZooKeeperNext.Client
{
    public class ZookeeperClient : Watcher, IZookeeperClient
    {
        #region Field

        private readonly ConcurrentDictionary<string, NodeEntry> _nodeEntries =
            new ConcurrentDictionary<string, NodeEntry>();

        private ConnectionStateChangeHandler _connectionStateChangeHandler;

        private Event.KeeperState _currentState;
        private readonly AutoResetEvent _stateChangedCondition = new AutoResetEvent(false);

        private readonly object _zkEventLock = new object();

        private bool _isDispose;

        #endregion Field

        #region Constructor

        /// <summary>
        /// ZookeeperClient
        /// </summary>
        /// <param name="connectionString"></param>
        public ZookeeperClient(string connectionString)
            : this(new ZookeeperClientOptions(connectionString))
        {
        }

        /// <summary>
        /// ZookeeperClient
        /// </summary>
        /// <param name="options">options</param>
        public ZookeeperClient(ZookeeperClientOptions options)
        {
            Options = options;
            ZooKeeper = CreateZooKeeper();
        }

        #endregion Constructor

        #region Public Method


        public org.apache.zookeeper.ZooKeeper ZooKeeper { get; private set; }

        public ZookeeperClientOptions Options { get; }

        public bool WaitForKeeperState(Event.KeeperState states, TimeSpan timeout)
        {
            var stillWaiting = true;
            while (_currentState != states)
            {
                if (!stillWaiting)
                {
                    return false;
                }

                stillWaiting = _stateChangedCondition.WaitOne(timeout);
            }
            return true;
        }

        public async Task<T> RetryUntilConnected<T>(Func<Task<T>> callable)
        {
            var operationStartTime = DateTime.Now;
            while (true)
            {
                try
                {
                    return await callable();
                }
                catch (KeeperException.ConnectionLossException)
                {
                    await Task.Yield();
                    this.WaitForRetry();
                }
                catch (KeeperException.SessionExpiredException)
                {
                    await Task.Yield();
                    this.WaitForRetry();
                }
                if (DateTime.Now - operationStartTime > Options.OperatingTimeout)
                {
                    throw new TimeoutException($"Operation cannot be retried because of retry timeout ({Options.OperatingTimeout.TotalMilliseconds} milli seconds)");
                }
            }
        }

        public async Task<IEnumerable<byte>> GetDataAsync(string path)
        {
            path = GetZooKeeperPath(path);

            var nodeEntry = GetOrAddNodeEntry(path);
            return await RetryUntilConnected(async () => await nodeEntry.GetDataAsync());
        }

        public async Task<IEnumerable<string>> GetChildrenAsync(string path)
        {
            path = GetZooKeeperPath(path);

            var nodeEntry = GetOrAddNodeEntry(path);
            return await RetryUntilConnected(async () => await nodeEntry.GetChildrenAsync());
        }

        public async Task<bool> ExistsAsync(string path)
        {
            path = GetZooKeeperPath(path);

            var nodeEntry = GetOrAddNodeEntry(path);
            return await RetryUntilConnected(async () => await nodeEntry.ExistsAsync());
        }

        public async Task<string> CreateAsync(string path, byte[] data, List<ACL> acls, CreateMode createMode)
        {
            path = GetZooKeeperPath(path);

            var nodeEntry = GetOrAddNodeEntry(path);
            return await RetryUntilConnected(async () => await nodeEntry.CreateAsync(data, acls, createMode));
        }

        public async Task<Stat> SetDataAsync(string path, byte[] data, int version = -1)
        {
            path = GetZooKeeperPath(path);

            var nodeEntry = GetOrAddNodeEntry(path);
            return await RetryUntilConnected(async () => await nodeEntry.SetDataAsync(data, version));
        }

        public async Task DeleteAsync(string path, int version = -1)
        {
            path = GetZooKeeperPath(path);

            var nodeEntry = GetOrAddNodeEntry(path);
            await RetryUntilConnected(async () =>
            {
                await nodeEntry.DeleteAsync(version);
                return 0;
            });
        }

        public async Task SubscribeDataChange(string path, NodeDataChangeHandler listener)
        {
            path = GetZooKeeperPath(path);

            var node = GetOrAddNodeEntry(path);
            await node.SubscribeDataChange(listener);
        }

        public void UnSubscribeDataChange(string path, NodeDataChangeHandler listener)
        {
            path = GetZooKeeperPath(path);

            var node = GetOrAddNodeEntry(path);
            node.UnSubscribeDataChange(listener);
        }

        public void SubscribeStatusChange(ConnectionStateChangeHandler listener)
        {
            _connectionStateChangeHandler += listener;
        }

        public void UnSubscribeStatusChange(ConnectionStateChangeHandler listener)
        {
            _connectionStateChangeHandler -= listener;
        }

        public async Task<IEnumerable<string>> SubscribeChildrenChange(string path, NodeChildrenChangeHandler listener)
        {
            path = GetZooKeeperPath(path);

            var node = GetOrAddNodeEntry(path);
            return await node.SubscribeChildrenChange(listener);
        }

        public void UnSubscribeChildrenChange(string path, NodeChildrenChangeHandler listener)
        {
            path = GetZooKeeperPath(path);

            var node = GetOrAddNodeEntry(path);
            node.UnSubscribeChildrenChange(listener);
        }

        #endregion Public Method

        #region Overrides of Watcher

        /// <summary>Processes the specified event.</summary>
        /// <param name="watchedEvent">The event.</param>
        /// <returns></returns>
        public override async Task process(WatchedEvent watchedEvent)
        {
            if (_isDispose)
                return;

            var path = watchedEvent.getPath();
            if (path == null)
            {
                await OnConnectionStateChange(watchedEvent);
            }
            else
            {
                NodeEntry nodeEntry;
                if (!_nodeEntries.TryGetValue(path, out nodeEntry))
                    return;
                await nodeEntry.OnChange(watchedEvent, false);
            }
        }

        #endregion Overrides of Watcher

        #region Implementation of IDisposable

        public void Dispose()
        {
            if (_isDispose)
                return;
            _isDispose = true;

            lock (_zkEventLock)
            {
                Task.Run(async () =>
                {
                    await ZooKeeper.closeAsync().ConfigureAwait(false);
                }).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        #endregion Implementation of IDisposable

        #region Private Method

        private bool _isFirstConnectioned = true;

        private async Task OnConnectionStateChange(WatchedEvent watchedEvent)
        {
            if (_isDispose)
                return;

            var state = watchedEvent.getState();
            SetCurrentState(state);

            if (state == Event.KeeperState.Expired)
            {
                await ReConnect();
            }
            else if (state == Event.KeeperState.SyncConnected)
            {
                if (_isFirstConnectioned)
                {
                    _isFirstConnectioned = false;
                }
                else
                {
                    foreach (var nodeEntry in _nodeEntries)
                    {
                        await nodeEntry.Value.OnChange(watchedEvent, true);
                    }
                }
            }

            _stateChangedCondition.Set();
            if (_connectionStateChangeHandler == null)
                return;
            await _connectionStateChangeHandler(this, new ConnectionStateChangeArgs
            {
                State = state
            });
        }

        private NodeEntry GetOrAddNodeEntry(string path)
        {
            return _nodeEntries.GetOrAdd(path, k => new NodeEntry(path, this));
        }

        private org.apache.zookeeper.ZooKeeper CreateZooKeeper()
        {
            return new org.apache.zookeeper.ZooKeeper(Options.ConnectionString, (int)Options.SessionTimeout.TotalMilliseconds, this, Options.SessionId, Options.SessionPasswd, Options.ReadOnly);
        }

        private async Task ReConnect()
        {
            if (!Monitor.TryEnter(_zkEventLock, Options.ConnectionTimeout))
                return;
            try
            {
                if (ZooKeeper != null)
                {
                    try
                    {
                        await ZooKeeper.closeAsync();
                    }
                    catch
                    {
                        // ignored
                    }
                }
                ZooKeeper = CreateZooKeeper();
            }
            finally
            {
                Monitor.Exit(_zkEventLock);
            }
        }

        private void SetCurrentState(Event.KeeperState state)
        {
            lock (this)
            {
                _currentState = state;
            }
        }

        private string GetZooKeeperPath(string path)
        {
            var basePath = Options.BasePath ?? "/";

            if (!basePath.StartsWith("/"))
                basePath = basePath.Insert(0, "/");

            basePath = basePath.TrimEnd('/');

            if (!path.StartsWith("/"))
                path = path.Insert(0, "/");

            path = $"{basePath}{path.TrimEnd('/')}";
            return string.IsNullOrEmpty(path) ? "/" : path;
        }

        #endregion Private Method
    }
}