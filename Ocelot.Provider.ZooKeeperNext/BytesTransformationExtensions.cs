using System.Text;

namespace Ocelot.Provider.ZooKeeperNext;

static class BytesTransformationExtensions
{
    public static byte[] ToBytesArray(this string entry)
    {
        return Encoding.UTF8.GetBytes(entry);
    }
}