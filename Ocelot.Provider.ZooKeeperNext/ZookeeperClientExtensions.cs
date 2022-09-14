namespace Ocelot.Provider.ZooKeeperNext
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Ocelot.Provider.ZooKeeperNext.Client;

    public static class ZookeeperClientExtensions
    {
        public static async Task<string> GetAsync(this IZookeeperClient client, string path)
        {
            if (await client.ExistsAsync(path))
            {
                return Encoding.UTF8.GetString((await client.GetDataAsync(path)).ToArray());
            }

            return null;
        }

        public static async Task<bool> SetAsync(this IZookeeperClient client, string path, string value)
        {
            if (await client.ExistsAsync(path))
            {
                var result = await client.SetDataAsync(path, Encoding.UTF8.GetBytes(value));
                return result != null;
            }

            await client.CreateRecursiveAsync(path, Encoding.UTF8.GetBytes(value));

            return true;
        }

        public static async Task<Dictionary<string, string>> GetRangeAsync(this IZookeeperClient client, string path)
        {
            var servicesRegistrySnapshot = new Dictionary<string, string>();

            var availableServices = await client.GetChildrenAsync(path);

            foreach (var availableServicesEntries in availableServices)
            {
                var servicesEntries = await client.GetChildrenAsync(path + $"/{availableServicesEntries}");

                foreach (var serviceEntry in servicesEntries)
                {
                    var serviceEntryPath = $"{path}/{availableServicesEntries}/{serviceEntry}";
                    var serviceData = await client.GetDataAsync(serviceEntryPath);
                    if (serviceData != null && serviceData.Any())
                    {
                        servicesRegistrySnapshot.Add(serviceEntry, Encoding.UTF8.GetString(serviceData.ToArray()));
                    }
                }
            }

            return servicesRegistrySnapshot;
        }

        public static async Task<bool> RegisterService(this IZookeeperClient client, ServiceEntry service, string keyOfServiceInZookeeper)
        {
            const string servicesBasePath = "/Ocelot/Services";
            bool exceptionOccured = false;
            try
            {
                var _entry = JsonConvert.SerializeObject(service);

                // path: /Ocelot/Services/configKey/sentry1/sentry1__682ze2xef4c8ze
                var servicePath = $"{servicesBasePath}/{keyOfServiceInZookeeper}/{service.Name}/{service.Id}";

                if (!await client.ExistsAsync(servicePath))
                    await client.CreateRecursiveAsync(path: servicePath, data: _entry.ToBytesArray());
                else
                    await client.SetDataAsync(path: servicePath, data: _entry.ToBytesArray());
            }
            catch (System.Exception)
            {
                exceptionOccured = true;
            }

            return !exceptionOccured;
        }

        public static async Task<bool> UnregisterServiceEntry(this IZookeeperClient client, string keyOfServiceInZookeeper, string serviceName, string serviceId)
        {
            const string servicesBasePath = "/Ocelot/Services";
            var servicePath = $"/{servicesBasePath}/{keyOfServiceInZookeeper}/{serviceName}/{serviceId}";
            return await client.DeleteRecursiveAsync(servicePath);
        }
    }
}