/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.temporal.runtime.utils;

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.Map;

/**
 * Utility methods for Temporal operations and data conversion.
 */
public class TemporalUtils {

    private TemporalUtils() {
        // Utility class
    }

    /**
     * Create a Ballerina error object.
     *
     * @param errorType Type of the error
     * @param message   Error message
     * @return Ballerina error object
     */
    public static BError createError(String errorType, String message) {
        return ErrorCreator.createError(StringUtils.fromString(errorType), StringUtils.fromString(message));
    }

    /**
     * Convert Ballerina array to Java array.
     *
     * @param bArray Ballerina array
     * @return Java object array
     */
    public static Object[] convertBArrayToJavaArray(BArray bArray) {
        if (bArray == null) {
            return new Object[0];
        }

        Object[] javaArray = new Object[bArray.size()];
        for (int i = 0; i < bArray.size(); i++) {
            javaArray[i] = convertBallerinaToJavaObject(bArray.get(i));
        }
        return javaArray;
    }

    /**
     * Convert Ballerina object to Java object.
     *
     * @param ballerinaObject Ballerina object
     * @return Java object
     */
    public static Object convertBallerinaToJavaObject(Object ballerinaObject) {
        if (ballerinaObject == null) {
            return null;
        }

        if (ballerinaObject instanceof BString) {
            return ((BString) ballerinaObject).getValue();
        }

        if (ballerinaObject instanceof Long || ballerinaObject instanceof Integer ||
                ballerinaObject instanceof Double || ballerinaObject instanceof Boolean) {
            return ballerinaObject;
        }

        // For complex objects, convert to string representation for now
        return ballerinaObject.toString();
    }

    /**
     * Convert Java object to Ballerina compatible object.
     *
     * @param javaObject Java object
     * @return Ballerina compatible object
     */
    public static Object convertJavaObjectToBallerina(Object javaObject) {
        if (javaObject == null) {
            return null;
        }

        if (javaObject instanceof String) {
            return StringUtils.fromString((String) javaObject);
        }

        if (javaObject instanceof Long || javaObject instanceof Integer || javaObject instanceof Double ||
                javaObject instanceof Boolean) {
            return javaObject;
        }

        // For complex objects, convert to string for now
        return StringUtils.fromString(javaObject.toString());
    }

    /**
     * Create a workflow execution record.
     *
     * @param executionData Execution data map
     * @return Ballerina record representing workflow execution
     */
    public static BMap<BString, Object> createExecutionRecord(Map<String, Object> executionData) {
        BMap<BString, Object> execution = ValueCreator.createMapValue();

        // Set the workflow ID
        Object id = executionData.get("id");
        if (id instanceof String) {
            execution.put(StringUtils.fromString("id"), StringUtils.fromString((String) id));
        }

        return execution;
    }

    /**
     * Validate Temporal configuration parameters.
     *
     * @param serviceUrl Service URL to validate
     * @param namespace  Namespace to validate
     * @throws IllegalArgumentException if parameters are invalid
     */
    public static void validateConfiguration(String serviceUrl, String namespace) {
        if (serviceUrl == null || serviceUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Service URL cannot be null or empty");
        }

        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace cannot be null or empty");
        }
    }

    /**
     * Extract connection parameters from Temporal configuration.
     *
     * @param config Temporal configuration map
     * @return Connection parameters map
     */
    public static Map<String, Object> extractConnectionParams(BMap<BString, Object> config) {
        Map<String, Object> params = new java.util.HashMap<>();

        if (config != null) {
            config.entrySet().forEach(entry -> {
                String key = entry.getKey().getValue();
                Object value = entry.getValue();

                if (value instanceof BString) {
                    params.put(key, ((BString) value).getValue());
                } else {
                    params.put(key, value);
                }
            });
        }

        return params;
    }
}
