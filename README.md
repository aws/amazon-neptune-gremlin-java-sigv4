## ⚠️ Deprecated

**This library is deprecated and no longer needed.** It was built in 2018 to work around a
limitation in Apache TinkerPop's Gremlin Java driver: there was no public extension point for
injecting custom (e.g. SigV4-signed) headers into the WebSocket handshake, so this package
shipped a custom `Channelizer` and `WebSocketClientHandshaker` to do it.

Apache TinkerPop has since added first-class support for this:

- `Cluster.Builder.handshakeInterceptor(...)`, added in TinkerPop 3.5.5
- `Cluster.Builder.requestInterceptor(...)`, added in TinkerPop 3.6.6

Every currently supported (non end-of-life) [Amazon Neptune engine release](https://docs.aws.amazon.com/neptune/latest/userguide/engine-releases.html)
requires a Gremlin client of at least TinkerPop 3.6.2. See the
[TinkerPop/Neptune engine version compatibility table](https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-gremlin.html)
for the minimum/maximum TinkerPop version supported by each Neptune engine release. Every
engine version in that table (1.1.1.0 and newer) supports a TinkerPop client that already has
`requestInterceptor()` (3.6.6+) or at least `handshakeInterceptor()` (3.5.5+), so every supported
Neptune cluster can already use one of these hooks directly. Only engine versions 1.1.0.0 and
older, themselves marked deprecated in that table, predate this, and there is no supported
Neptune version for which this library is still required.

**Use [`amazon-neptune-sigv4-signer`](https://github.com/aws/amazon-neptune-sigv4-signer) directly
with `Cluster.Builder.requestInterceptor(...)` instead.** See [Migration](#migration) below, and
the official guide: [Connecting to Amazon Neptune databases using IAM with Gremlin Java](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html).

This repository is archived and will not receive further updates, including dependency or
security patches. Previously published artifacts remain available on Maven Central for existing
consumers, but new projects should not take a dependency on this library.

## Migration

Replace the `SigV4WebSocketChannelizer` channelizer with a `requestInterceptor` that signs each
request using `amazon-neptune-sigv4-signer` directly. No dependency on this package
(`amazon-neptune-gremlin-java-sigv4`) is needed.

**Before** (using this library):

```java
Cluster.Builder builder = Cluster.build();
builder.addContactPoint(endpoint);
builder.port(port);
builder.channelizer(SigV4WebSocketChannelizer.class);
builder.enableSsl(true);
Cluster cluster = builder.create();
```

**After** (TinkerPop 3.6.6+, no channelizer required):

```java
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.tinkerpop.gremlin.driver.Cluster;

Cluster cluster = Cluster.build()
        .addContactPoint(endpoint)
        .port(port)
        .enableSsl(true)
        .requestInterceptor(r -> {
            try {
                NeptuneNettyHttpSigV4Signer sigV4Signer =
                        new NeptuneNettyHttpSigV4Signer("your-region", DefaultCredentialsProvider.create());
                sigV4Signer.signRequest(r);
            } catch (NeptuneSigV4SignerException e) {
                throw new RuntimeException("Exception occurred while signing the request", e);
            }
            return r;
        })
        .create();
```

If you are using a TinkerPop version between 3.5.5 and 3.6.5 (inclusive), use
`handshakeInterceptor(...)` instead of `requestInterceptor(...)`; the signature is the same.

Add the signer dependency in place of this one:

```xml
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>amazon-neptune-sigv4-signer</artifactId>
    <version>5.0.0</version>
</dependency>
```

`amazon-neptune-sigv4-signer` uses AWS SDK for Java v2 credential providers
(`software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`). If you were relying on this
library's `ChainedSigV4PropertiesProvider` to resolve the region from an environment variable or
system property, you can keep doing that yourself, or pass the region directly as shown above.

For cross-account IAM authentication and other advanced scenarios, see
[Connecting to Amazon Neptune databases using IAM with Gremlin Java](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html).

---

## Amazon Neptune Gremlin Java Sigv4 (archived)

An extension to GremlinDriver with a custom channelizer that enables AWS Signature Version 4 signed requests to [Amazon Neptune](https://aws.amazon.com/neptune). 
 
For example usage refer to:
 
- [NeptuneGremlinSigV4Example.java](https://github.com/aws/amazon-neptune-gremlin-java-sigv4/blob/master/src/main/java/com/amazon/neptune/gremlin/driver/example/NeptuneGremlinSigV4Example.java): This package can also be used to enable Gremlin Console to send signed requests to Neptune, refer to [Connecting to Neptune Using the Gremlin Console with Signature Version 4 Signing](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-console.html).
- If you are using versions of TinkerPop after 3.4.11 or higher you should prefer using the [amazon-neptune-sigv4-signer](https://github.com/aws/amazon-neptune-sigv4-signer) directly as discussed [here](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html#iam-auth-connecting-gremlin-java-current). 

For the official Amazon Neptune page refer to: https://aws.amazon.com/neptune

## Version

1.x - This series uses TinkerPop 3.3.x client. Note that active maintenance on TinkerPop 3.3.x has stopped and hence, this version is not recommended.

2.x - This series uses TinkerPop 3.4.x client. This major version tracks the latest stable release for this package. Note that a minor version (y in 2.x.y) is bumped whenever a new version of Apache TinkerPop is added as a dependency or a major feature is introduced. All minor versions in 2.x series are backward compatible.

3.x - This series continues use of TinkerPop 3.4.x client and was incremented to simply match the major increment to the [amazon-neptune-sigv4-signer](https://github.com/aws/amazon-neptune-sigv4-signer).

Releases of [amazon-neptune-sigv4-signer](https://github.com/aws/amazon-neptune-sigv4-signer) after 3.x no longer track version with this library as the signer can be used directly for request signing in the driver, as mentioned above. For more information on compatibility with Amazon Neptune engine releases, see [Use the Latest Version of the Gremlin Java Client](https://docs.aws.amazon.com/neptune/latest/userguide/best-practices-gremlin-java-latest.html).

## License

This library is licensed under the Apache 2.0 License. 
