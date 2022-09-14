using System;

namespace Ocelot.Provider.ZooKeeperNext.Client
{
    public class ZookeeperClientOptions
    {
        public ZookeeperClientOptions()
        {
            ConnectionTimeout = TimeSpan.FromSeconds(10);
            SessionTimeout = TimeSpan.FromSeconds(20);
            OperatingTimeout = TimeSpan.FromSeconds(60);
            ReadOnly = false;
            SessionId = 0;
            SessionPasswd = null;
            EnableEphemeralNodeRestore = true;
        }

        public ZookeeperClientOptions(string connectionString) : this()
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            ConnectionString = connectionString;
        }

        public string ConnectionString { get; set; }

        public TimeSpan ConnectionTimeout { get; set; }

        public TimeSpan OperatingTimeout { get; set; }

        public TimeSpan SessionTimeout { get; set; }

        public bool ReadOnly { get; set; }

        public long SessionId { get; set; }

        public byte[] SessionPasswd { get; set; }

        public string BasePath { get; set; }

        public bool EnableEphemeralNodeRestore { get; set; }
    }
}