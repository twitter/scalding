# Using the Distributed Cache

The distributed cache is simply hadoop's method for allowing each node local access to a specific file. In the example, I am mapping ip addresses to geographical locations (country, city, etc.). The heavy lifting is done my Maxmind's geoip LookupService. LookupService requires random access to a local file, GeoLiteCity.dat, which defines a mapping from ip ranges to a location object.

The DistributedCacheFile object provides a simple API for dealing with registering files to be copied into the file cache at configuration time, and accessing them later during map/reduce time.

Firstly, we need to place the file in question on the hadoop file system.
I used [s3distcp](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_s3distcp.html) to put a copy of GeoLiteCity.dat to hdfs://cache/GeoLiteCity.dat. If your copy is not on s3, you can use the normal distcp command.

So now GeoLiteCity.dat is distributed across the data nodes. So let now go to the scalding code.

```scala
import com.twitter.scalding.filecache.DistributedCacheFile

val geoIPDataFile = DistributedCacheFile("hdfs://path/to/maxmind.dat")

```

This handles registering the file in the DistributedCache for us at configuration time, and will work in both local and hdfs modes (in local mode, it simply uses the local path and creates no symlinks).

Next, we need to be very careful about how we instantiate LookupService. Lookup service is not serializable, so we cannot be holding a reference when the job get serialized to the nodes. Also we don't want to be initializing it more than once per task attempt.

```scala
 @transient private lazy val lookupService =
 	new LookupService(geoIPDataFile.path, LookupService.GEOIP_MEMORY_CACHE)
```

By making lookupService transient, we avoid serialization, but it means that it will be null when received by the nodes. Making it lazy ensures that the nodes will initalize it when needed. We use the auto-generated symlink path created by DistributedCacheFile. This does not require the jobConf as it will not be availible, when lookupService is lazily evaluated.

Lastly, we write a function to be used by our mappers to do the lookup. This is included for completeness:
```scala
def getLocationInfo(ip: String): (String, String, String, String, String, Int) = {
    val unknown = ("", "", "", "", "", 0)
    if (ip == "" || ip == null)
      return unknown
    val location = lookupService.getLocation(ip)
    if (location == null)
      return unknown
    (location.countryName,
     location.countryCode,
     location.region,
     location.city,
     location.postalCode,
     location.metro_code)
  }
```

