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

package com.amazon.neptune.gremlin.driver.sigv4;

import java.net.URI;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

/**
 * Extends the functionality of {@link WebSocketClientHandshaker13} by adding SIGV4 authentication headers to the
 * request.
 */
public class AwsSigV4ClientHandshaker extends WebSocketClientHandshaker13 {
    /**
     * Instance of the properties provider to get properties for SIGV4 signing.
     */
    private final ChainedSigV4PropertiesProvider sigV4PropertiesProvider;

    /**
     * SigV4 properties required to perform the signing.
     */
    private final SigV4Properties sigV4Properties;

    /**
     * Credentials provider to use to generate signature
     */
    private final AWSCredentialsProvider awsCredentialsProvider;

    /**
     * Creates a new instance with default credentials provider (for backward compatibility).
     * @param webSocketURL - URL for web socket communications. e.g "ws://myhost.com/mypath". Subsequent web socket
     * frames will be sent to this URL.
     * @param version - Version of web socket specification to use to connect to the server
     * @param subprotocol - Sub protocol request sent to the server.
     * @param allowExtensions - Allow extensions to be used in the reserved bits of the web socket frame
     * @param customHeaders - Map of custom headers to add to the client request
     * @param maxFramePayloadLength - Maximum length of a frame's payload
     * @param sigV4PropertiesProvider - a properties provider to get sigV4 auth related properties
     */
    @Deprecated
    public AwsSigV4ClientHandshaker(final URI webSocketURL,
                                    final WebSocketVersion version,
                                    final String subprotocol,
                                    final boolean allowExtensions,
                                    final HttpHeaders customHeaders,
                                    final int maxFramePayloadLength,
                                    final ChainedSigV4PropertiesProvider sigV4PropertiesProvider) {
        this(
            webSocketURL,
            version,
            subprotocol,
            allowExtensions,
            customHeaders,
            maxFramePayloadLength,
            sigV4PropertiesProvider,
            new DefaultAWSCredentialsProviderChain()
        );
    }

    /**
     * Creates a new instance.
     * @param webSocketURL - URL for web socket communications. e.g "ws://myhost.com/mypath". Subsequent web socket
     * frames will be sent to this URL.
     * @param version - Version of web socket specification to use to connect to the server
     * @param subprotocol - Sub protocol request sent to the server.
     * @param allowExtensions - Allow extensions to be used in the reserved bits of the web socket frame
     * @param customHeaders - Map of custom headers to add to the client request
     * @param maxFramePayloadLength - Maximum length of a frame's payload
     * @param sigV4PropertiesProvider - a properties provider to get sigV4 auth related properties
     * @param awsCredentialsProvider - an AWS credentials provider to use to generate signature
     */
    public AwsSigV4ClientHandshaker(final URI webSocketURL,
                                    final WebSocketVersion version,
                                    final String subprotocol,
                                    final boolean allowExtensions,
                                    final HttpHeaders customHeaders,
                                    final int maxFramePayloadLength,
                                    final ChainedSigV4PropertiesProvider sigV4PropertiesProvider,
                                    final AWSCredentialsProvider awsCredentialsProvider
                                    ) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength);
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.sigV4PropertiesProvider = sigV4PropertiesProvider;
        this.sigV4Properties = loadProperties();
    }

    /**
     * Gets the request as generated by {@link WebSocketClientHandshaker13} and adds additional headers and a SIGV4
     * signature required for SigV4Auth.
     * Sends the opening request to the server:
     * GET /chat HTTP/1.1
     * Host: server.example.com
     * Upgrade: websocket
     * Connection: Upgrade
     * Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
     * Sec-WebSocket-Origin: http://example.com
     * Sec-WebSocket-Protocol: chat, superchat
     * Sec-WebSocket-Version: 13
     * x-amz-date: 20180214T002049Z
     * Authorization: [SIGV4AuthHeader]
     * @return SIGV4 signed {@link FullHttpRequest}.
     */
    @Override
    protected FullHttpRequest newHandshakeRequest() {
        final FullHttpRequest request = super.newHandshakeRequest();
        final NeptuneNettyHttpSigV4Signer sigV4Signer;
        try {
            sigV4Signer = new NeptuneNettyHttpSigV4Signer(this.sigV4Properties.getServiceRegion(),
                    awsCredentialsProvider);
            sigV4Signer.signRequest(request);
        } catch (NeptuneSigV4SignerException e) {
            throw new RuntimeException("Exception occurred while signing the request", e);
        }
        return request;
    }

    /**
     * Calls the {@link ChainedSigV4PropertiesProvider} to get the properties required for SigV4 signing.
     * @return an instance of {@link SigV4Properties}.
     */
    private SigV4Properties loadProperties() {
        return sigV4PropertiesProvider.getSigV4Properties();
    }
}

