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

package com.amazon.neptune.gremlin.driver.exception;

/**
 * Denotes an exception when trying to extract properties required for SigV4 signing.
 */
public class SigV4PropertiesNotFoundException extends RuntimeException {
    /**
     * @param message the error message.
     */
    public SigV4PropertiesNotFoundException(final String message) {
        super(message);
    }

    /**
     * @param message the error message.
     * @param cause the root cause exception.
     */
    public SigV4PropertiesNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
