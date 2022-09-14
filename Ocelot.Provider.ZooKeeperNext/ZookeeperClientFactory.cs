using Ocelot.Provider.ZooKeeperNext.Client;

namespace Ocelot.Provider.ZooKeeperNext
{
    public class ZookeeperClientFactory : IZookeeperClientFactory
    {
        public ZookeeperClient Get(ZookeeperRegistryConfiguration config)
        {
            return new ZookeeperClient($"{config.Host}:{config.Port}");
        }
    }
}