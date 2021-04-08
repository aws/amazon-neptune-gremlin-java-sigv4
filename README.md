## Amazon Neptune Gremlin Java Sigv4

An extension to GremlinDriver with a custom channelizer that enables AWS Signature Version 4 signed requests to [Amazon Neptune](https://aws.amazon.com/neptune). 
 
For example usage refer to:
 
-	[NeptuneGremlinSigV4Example.java](https://github.com/aws/amazon-neptune-gremlin-java-sigv4/blob/master/src/main/java/com/amazon/neptune/gremlin/driver/example/NeptuneGremlinSigV4Example.java): This package can also be used to enable Gremlin Console to send signed requests to Neptune, refer to [Connecting to Neptune Using the Gremlin Console with Signature Version 4 Signing](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-console.html).

For the official Amazon Neptune page refer to: https://aws.amazon.com/neptune

## Version

1.x - This series uses TinkerPop 3.3.x client. Note that active maintenance on TinkerPop 3.3.x has stopped and hence, this version is not recommended.

2.x - This series uses TinkerPop 3.4.x client. This major version tracks the latest stable release for this package. Note that a minor version (y in 2.x.y) is bumped whenever a new version of Apache TinkerPop is added as a dependency or a major feature is introduced. All minor versions in 2.x series are backward compatible.

For more information on compatibility with Amazon Neptune engine releases, see [Use the Latest Version of the Gremlin Java Client](https://docs.aws.amazon.com/neptune/latest/userguide/best-practices-gremlin-java-latest.html). 

## License

This library is licensed under the Apache 2.0 License. 
