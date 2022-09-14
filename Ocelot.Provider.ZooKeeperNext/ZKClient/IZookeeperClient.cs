using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ocelot.Provider.ZooKeeperNext.Client
{
    public interface IZookeeperClient : IDisposable
    {
        org.apache.zookeeper.ZooKeeper ZooKeeper { get; }

        ZookeeperClientOptions Options { get; }

        bool WaitForKeeperState(Watcher.Event.KeeperState states, TimeSpan timeout);

        Task<T> RetryUntilConnected<T>(Func<Task<T>> callable);

        Task<IEnumerable<byte>> GetDataAsync(string path);

        Task<IEnumerable<string>> GetChildrenAsync(string path);

        Task<bool> ExistsAsync(string path);

        Task<string> CreateAsync(string path, byte[] data, List<ACL> acls, CreateMode createMode);

        Task<Stat> SetDataAsync(string path, byte[] data, int version = -1);

        Task DeleteAsync(string path, int version = -1);

        Task SubscribeDataChange(string path, NodeDataChangeHandler listener);

        void UnSubscribeDataChange(string path, NodeDataChangeHandler listener);

        void SubscribeStatusChange(ConnectionStateChangeHandler listener);

        void UnSubscribeStatusChange(ConnectionStateChangeHandler listener);

        Task<IEnumerable<string>> SubscribeChildrenChange(string path, NodeChildrenChangeHandler listener);

        void UnSubscribeChildrenChange(string path, NodeChildrenChangeHandler listener);
    }

    public static class ZookeeperClientExtensions
    {

        public static Task<string> CreateEphemeralAsync(this IZookeeperClient client, string path, byte[] data, bool isSequential = false)
        {
            return client.CreateEphemeralAsync(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, isSequential);
        }

        public static Task<string> CreateEphemeralAsync(this IZookeeperClient client, string path, byte[] data, List<ACL> acls, bool isSequential = false)
        {
            return client.CreateAsync(path, data, acls, isSequential ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.EPHEMERAL);
        }

        public static Task<string> CreatePersistentAsync(this IZookeeperClient client, string path, byte[] data, bool isSequential = false)
        {
            return client.CreatePersistentAsync(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, isSequential);
        }

        public static Task<string> CreatePersistentAsync(this IZookeeperClient client, string path, byte[] data, List<ACL> acls, bool isSequential = false)
        {
            return client.CreateAsync(path, data, acls, isSequential ? CreateMode.PERSISTENT_SEQUENTIAL : CreateMode.PERSISTENT);
        }

        public static async Task<bool> DeleteRecursiveAsync(this IZookeeperClient client, string path)
        {
            IEnumerable<string> children;
            try
            {
                children = await client.GetChildrenAsync(path);
            }
            catch (KeeperException.NoNodeException)
            {
                return true;
            }

            foreach (var subPath in children)
            {
                if (!await client.DeleteRecursiveAsync(path + "/" + subPath))
                {
                    return false;
                }
            }
            await client.DeleteAsync(path);
            return true;
        }

        public static Task CreateRecursiveAsync(this IZookeeperClient client, string path, byte[] data)
        {
            return client.CreateRecursiveAsync(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

        public static Task CreateRecursiveAsync(this IZookeeperClient client, string path, byte[] data, List<ACL> acls)
        {
            return client.CreateRecursiveAsync(path, p => data, p => acls);
        }

        public static async Task CreateRecursiveAsync(this IZookeeperClient client, string path, Func<string, byte[]> getNodeData, Func<string, List<ACL>> getNodeAcls)
        {
            var data = getNodeData(path);
            var acls = getNodeAcls(path);
            try
            {
                await client.CreateAsync(path, data, acls, CreateMode.PERSISTENT);
            }
            catch (KeeperException.NodeExistsException)
            {
            }
            catch (KeeperException.NoNodeException)
            {
                var parentDir = path.Substring(0, path.LastIndexOf('/'));
                await CreateRecursiveAsync(client, parentDir, getNodeData, getNodeAcls);
                await client.CreateAsync(path, data, acls, CreateMode.PERSISTENT);
            }
        }

        public static void WaitForRetry(this IZookeeperClient client)
        {
            client.WaitUntilConnected(client.Options.OperatingTimeout);
        }

        public static bool WaitUntilConnected(this IZookeeperClient client, TimeSpan timeout)
        {
            return client.WaitForKeeperState(Watcher.Event.KeeperState.SyncConnected, timeout);
        }
    }
}