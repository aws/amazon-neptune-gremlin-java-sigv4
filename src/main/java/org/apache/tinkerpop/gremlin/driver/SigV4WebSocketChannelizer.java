/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.driver;

import com.amazon.neptune.gremlin.driver.sigv4.AwsSigV4ClientHandshaker;
import com.amazon.neptune.gremlin.driver.sigv4.ChainedSigV4PropertiesProvider;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import org.apache.tinkerpop.gremlin.driver.Channelizer.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;

import java.util.concurrent.TimeoutException;

/**
 * An {@link AbstractChannelizer}, with most of the code from {@link WebSocketChannelizer}. Except it uses a
 * different WebSocketClientHandshaker which uses SIGV4 auth. This class should be used as a Channelizer when SIGV4
 * auth is enabled.
 *
 * @see <a href="https://github.com/apache/tinkerpop/blob/master/gremlin-driver/src/main/java/org/apache/tinkerpop/gremlin/driver/Channelizer.java">
 *      https://github.com/apache/tinkerpop/blob/master/gremlin-driver/src/main/java/org/apache/tinkerpop/gremlin/driver/Channelizer.java</a>
 */
public class SigV4WebSocketChannelizer extends AbstractChannelizer {
    /**
     * Constant to denote the websocket protocol.
     */
    private static final String WEB_SOCKET = "ws";

    /**
     * Constant to denote the websocket secure protocol.
     */

    private static final String WEB_SOCKET_SECURE = "wss";
    /**
     * Name of the HttpCodec handler.
     */

    private static final String HTTP_CODEC = "http-codec";
    /**
     * Name of the HttpAggregator handler.
     */

    private static final String AGGREGATOR = "aggregator";
    /**
     * Name of the WebSocket handler.
     */

    private static final String WEB_SOCKET_HANDLER = "ws-handler";
    /**
     * Name of the GremlinEncoder handler.
     */

    private static final String GREMLIN_ENCODER = "gremlin-encoder";
    /**
     * Name of the GremlinDecoder handler.
     */
    private static final String GRELIN_DECODER = "gremlin-decoder";

    /**
     * Name of the WebSocket compression handler.
     */
    public static final String WEBSOCKET_COMPRESSION_HANDLER = "web-socket-compression-handler";

    /**
     * The handler to process websocket messages from the server.
     */
    private WebSocketClientHandler handler;

    /**
     * Encoder to encode websocket requests.
     */
    private WebSocketGremlinRequestEncoder webSocketGremlinRequestEncoder;

    /**
     * Decoder to decode websocket requests.
     */
    private WebSocketGremlinResponseDecoder webSocketGremlinResponseDecoder;

    /**
     * Initializes the channelizer.
     * @param connection the {@link Connection} object.
     */
    @Override
    public void init(final Connection connection) {
        super.init(connection);
        webSocketGremlinRequestEncoder = new WebSocketGremlinRequestEncoder(true, cluster.getSerializer());
        webSocketGremlinResponseDecoder = new WebSocketGremlinResponseDecoder(cluster.getSerializer());
    }

    /**
     * Keep-alive is supported through the ping/pong websocket protocol.
     * @see <a href=https://tools.ietf.org/html/rfc6455#section-5.5.2>IETF RFC 6455</a>
     */
    @Override
    public boolean supportsKeepAlive() {
        return true;
    }

    @Override
    public Object createKeepAliveMessage() {
        return new PingWebSocketFrame();
    }

    /**
     * Sends a {@code CloseWebSocketFrame} to the server for the specified channel.
     */
    @Override
    public void close(final Channel channel) {
        if (channel.isOpen()) {
            channel.writeAndFlush(new CloseWebSocketFrame());
        }
    }

    @Override
    public boolean supportsSsl() {
        final String scheme = connection.getUri().getScheme();
        return "wss".equalsIgnoreCase(scheme);
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        final String scheme = connection.getUri().getScheme();
        if (!WEB_SOCKET.equalsIgnoreCase(scheme) && !WEB_SOCKET_SECURE.equalsIgnoreCase(scheme)) {
            throw new IllegalStateException(String.format("Unsupported scheme (only %s: or %s: supported): %s",
                    WEB_SOCKET, WEB_SOCKET_SECURE, scheme));
        }

        if (!supportsSsl() && WEB_SOCKET_SECURE.equalsIgnoreCase(scheme)) {
            throw new IllegalStateException(String.format("To use %s scheme ensure that enableSsl is set to true in "
                            + "configuration",
                    WEB_SOCKET_SECURE));
        }

        final int maxContentLength = cluster.connectionPoolSettings().maxContentLength;
        handler = createHandler();

        pipeline.addLast(HTTP_CODEC, new HttpClientCodec());
        pipeline.addLast(AGGREGATOR, new HttpObjectAggregator(maxContentLength));
        // Add compression extension for WebSocket defined in https://tools.ietf.org/html/rfc7692
        pipeline.addLast(WEBSOCKET_COMPRESSION_HANDLER, WebSocketClientCompressionHandler.INSTANCE);
        pipeline.addLast(WEB_SOCKET_HANDLER, handler);
        pipeline.addLast(GREMLIN_ENCODER, webSocketGremlinRequestEncoder);
        pipeline.addLast(GRELIN_DECODER, webSocketGremlinResponseDecoder);
    }

    @Override
    public void connected() {
        try {
            // Block until the handshake is complete either successfully or with an error. The handshake future
            // will complete with a timeout exception after some time so it is guaranteed that this future will
            // complete. The timeout for the handshake is configured by cluster.getConnectionSetupTimeout().
            //
            // If future completed with an exception more than likely, SSL is enabled on the server, but the client
            // forgot to enable it or perhaps the server is not configured for websockets.
            handler.handshakeFuture().sync();
        } catch (Exception ex) {
            final String errMsg;
            if (ex instanceof TimeoutException) {
                // Note that we are not using catch(TimeoutException ex) because the compiler throws an error for
                // catching a checked exception which is not thrown from the code inside try. However, the compiler
                // check is incorrect since Netty bypasses the compiler check and sync() is able to rethrow underlying
                // exception even if it is a check exception.
                // More information about how Netty bypasses compiler check at https://github.com/netty/netty/blob/d371b1bbaa3b98f957f6b025673098ad3adb5131/common/src/main/java/io/netty/util/internal/PlatformDependent.java#L418
                errMsg = "Timed out while waiting to complete the connection setup. Consider increasing the " +
                        "WebSocket handshake timeout duration.";
            } else {
                errMsg = "Could not complete connection setup to the server. Ensure that SSL is correctly " +
                        "configured at both the client and the server. Ensure that client WebSocket handshake " +
                        "protocol matches the server. Ensure that the server is still reachable.";
            }
            throw new ConnectionException(connection.getUri(), errMsg, ex);
        }
    }

    /**
     * This protected method provides a way for customizing the channelize through inheritance
     * to override credentials used to establish sign requests
     *
     * @return credentials provider that will be used to generate SigV4 signatures
     */
    protected AWSCredentialsProvider getCredentialsProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }

    /**
     * Creates an instance of {@link WebSocketClientHandler} with {@link AwsSigV4ClientHandshaker} as the handshaker
     * for SigV4 auth.
     * @return the instance of clientHandler.
     */
    private WebSocketClientHandler createHandler() {

        WebSocketClientHandshaker handshaker = new AwsSigV4ClientHandshaker(
                connection.getUri(),
                WebSocketVersion.V13,
                null,
                true, // allow extensions to support WebSocket compression
                EmptyHttpHeaders.INSTANCE,
                cluster.getMaxContentLength(),
                new ChainedSigV4PropertiesProvider(),
                getCredentialsProvider());
        return new WebSocketClientHandler(handshaker, cluster.getConnectionSetupTimeout());
    }
}
