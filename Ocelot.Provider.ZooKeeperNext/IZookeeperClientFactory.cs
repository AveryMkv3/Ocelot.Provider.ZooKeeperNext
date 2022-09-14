using Ocelot.Provider.ZooKeeperNext.Client;

namespace Ocelot.Provider.ZooKeeperNext
{
    public interface IZookeeperClientFactory
    {
        ZookeeperClient Get(ZookeeperRegistryConfiguration config);
    }
}