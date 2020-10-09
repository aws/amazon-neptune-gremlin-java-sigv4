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

import com.amazon.neptune.gremlin.driver.exception.SigV4PropertiesNotFoundException;

import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * A chained Sig4Properties provider.
 * It tries to get the properties from environment variables and if not found looks in Java system properties.
 */
@Slf4j
public class ChainedSigV4PropertiesProvider {

    /**
     * The chain of provider lambdas that can read properties required for SigV4 signing.
     */
    private final Supplier<SigV4Properties>[] providers;

    /**
     * Creates an instance with default suppliers.
     */
    public ChainedSigV4PropertiesProvider() {
        this.providers = new Supplier[]{this::getSigV4PropertiesFromEnv, this::getSigV4PropertiesFromSystem};
    }

    /**
     * Creates an instance with the supplied chain of {@link SigV4Properties} providers.
     * @param providers the chain of sigv4 properties provider.
     */
    public ChainedSigV4PropertiesProvider(final Supplier<SigV4Properties>[] providers) {
        this.providers = new Supplier[providers.length];
        System.arraycopy(providers, 0, this.providers, 0, providers.length);
    }

    /**
     * Gets the {@link SigV4Properties} from the chain of lambdas.
     * @return the {@link SigV4Properties}.
     * @throws SigV4PropertiesNotFoundException when SigV4 properties are not set.
     */
    public SigV4Properties getSigV4Properties() throws SigV4PropertiesNotFoundException {
        SigV4Properties properties;

        for (Supplier<SigV4Properties> provider : providers) {
            try {
                properties = provider.get();
                log.info("Successfully loaded SigV4 properties from provider: {}", provider.getClass());
                return properties;
            } catch (SigV4PropertiesNotFoundException e) {
                log.info("Unable to load SigV4 properties from provider: {}", provider.getClass());
            }
        }

        final String message = "Unable to load SigV4 properties from any of the providers";
        log.warn(message);
        throw new SigV4PropertiesNotFoundException(message);
    }

    /**
     * Reads the SigV4 properties from the environment properties and constructs the {@link SigV4Properties} object.
     * @return the {@link SigV4Properties} constructed from system properties.
     * @throws SigV4PropertiesNotFoundException when properties are not found in the environment variables.
     */
    public SigV4Properties getSigV4PropertiesFromEnv() throws SigV4PropertiesNotFoundException {
        final String serviceRegion = StringUtils.trim(System.getenv(SigV4Properties.SERVICE_REGION));

        if (StringUtils.isBlank(serviceRegion)) {
            final String msg = "SigV4 properties not found as a environment variable";
            log.info(msg);
            throw new SigV4PropertiesNotFoundException(msg);
        }

        return new SigV4Properties(serviceRegion);
    }

    /**
     * Reads the SigV4 properties from the system properties and constructs the {@link SigV4Properties} object.
     * @return the {@link SigV4Properties} constructed from system properties.
     * @throws SigV4PropertiesNotFoundException when the properties are not found in the system properties.
     */
    public SigV4Properties getSigV4PropertiesFromSystem() {
        final String serviceRegion = StringUtils.trim(System.getProperty(SigV4Properties.SERVICE_REGION));

        if (StringUtils.isBlank(serviceRegion)) {
            final String msg = "SigV4 properties not found in system properties";
            log.info(msg);
            throw new SigV4PropertiesNotFoundException(msg);
        }

        return new SigV4Properties(serviceRegion);
    }
}
