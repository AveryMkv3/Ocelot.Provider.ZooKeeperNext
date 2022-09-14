namespace Ocelot.Provider.ZooKeeperNext
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Infrastructure.Extensions;
    using Logging;
    using Newtonsoft.Json;
    using Ocelot.Provider.ZooKeeperNext.Client;
    using ServiceDiscovery.Providers;
    using Values;

    public class Zookeeper : IServiceDiscoveryProvider
    {
        private readonly ZookeeperRegistryConfiguration _config;
        private readonly IOcelotLogger _logger;
        private readonly ZookeeperClient _zookeeperClient;
        private const string VersionPrefix = "version-";

        public Zookeeper(ZookeeperRegistryConfiguration config, IOcelotLoggerFactory factory, IZookeeperClientFactory clientFactory)
        {
            _logger = factory.CreateLogger<Zookeeper>();
            _config = config;
            _zookeeperClient = clientFactory.Get(_config);
        }

        public async Task<List<Service>> Get()
        {
            // path --> /Ocelot/Services/serviceName/serviceId
            var queryPath = $"/Ocelot/Services/{_config.KeyOfServiceInZookeeper}";
            var queryResult = await _zookeeperClient.GetRangeAsync(queryPath);

            var services = new List<Service>();

            foreach (var serviceEntrySnapshot in queryResult.ToList())
            {
                var serviceEntry = JsonConvert.DeserializeObject<ServiceEntry>(serviceEntrySnapshot.Value);  
                if (IsValid(serviceEntry))
                {
                    services.Add(BuildService(serviceEntry));
                }
                else
                {
                    _logger.LogWarning($"Unable to use service Address: {serviceEntry.Host} and Port: {serviceEntry.Port} as they are invalid. Address must contain host only e.g. localhost and port must be greater than 0");
                }
            }

            return services.ToList();
        }

        private Service BuildService(ServiceEntry serviceEntry)
        {
            return new Service(
                serviceEntry.Name,
                new ServiceHostAndPort(serviceEntry.Host, serviceEntry.Port),
                serviceEntry.Id,
              string.IsNullOrWhiteSpace(serviceEntry.Version) ? GetVersionFromStrings(serviceEntry.Tags) : serviceEntry.Version,
                serviceEntry.Tags ?? Enumerable.Empty<string>());
        }

        private bool IsValid(ServiceEntry serviceEntry)
        {
            if (string.IsNullOrEmpty(serviceEntry.Host) || serviceEntry.Host.Contains("http://") || serviceEntry.Host.Contains("https://") || serviceEntry.Port <= 0)
            {
                return false;
            }
            return true;
        }

        private string GetVersionFromStrings(IEnumerable<string> strings)
        {
            return strings
                ?.FirstOrDefault(x => x.StartsWith(VersionPrefix, StringComparison.Ordinal))
                .TrimStart(VersionPrefix);
        }
    }
}