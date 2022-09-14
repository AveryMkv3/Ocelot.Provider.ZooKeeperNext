namespace Ocelot.Provider.ZooKeeperNext
{
    using Errors;

    public class UnableToSetConfigInZookeeperError : Error
    {
        public UnableToSetConfigInZookeeperError(string s)
            : base(message: s, code: OcelotErrorCode.UnknownError, httpStatusCode: 500)
        {
        }
    }
}