/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.ballerina.stdlib.temporal;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.temporal.common.converter.EncodedValues;

/**
 * Utility class for type-guided conversion of EncodedValues to Ballerina types.
 */
public class TypeConversionUtils {

    private TypeConversionUtils() {
        // Utility class
    }

    /**
     * Converts EncodedValues to Ballerina-compatible objects based on workflow model data.
     * Uses parameter type information from the workflow definition to perform type-safe conversion.
     */
    public static Object[] convertEncodedValuesToBallerina(WorkerEngine engine, String workflowType,
                                                         String methodName, EncodedValues args) {
        // Step 1: Get the workflow model data
        BMap<BString, Object> modelData = engine.getWorkflowModelData(workflowType);
        if (modelData == null) {
            throw new IllegalStateException("No workflow model data found for type: " + workflowType);
        }

        // Step 2: Extract parameter types for the method
        BArray parameters = extractMethodParameters(modelData, methodName);
        if (parameters == null) {
            throw new IllegalStateException("No parameter type information found for method: " + methodName);
        }

        // Step 3: Convert each argument based on its parameter type
        Object[] result = new Object[args.getSize()];
        for (int i = 0; i < args.getSize() && i < parameters.size(); i++) {
            Object paramType = parameters.get(i);
            result[i] = convertEncodedValueByType(args, i, paramType);
        }

        return result;
    }

    /**
     * Extracts method parameters from workflow model data.
     */
    public static BArray extractMethodParameters(BMap<BString, Object> modelData, String methodName) {
        try {
            Object methodAction = null;

            // Handle different method types based on the WorkflowModelData structure
            if ("execute".equals(methodName)) {
                // execute is directly a WorkflowAction
                methodAction = modelData.get(StringUtils.fromString("execute"));
            } else {
                // Check if it's a signal
                Object signalsMap = modelData.get(StringUtils.fromString("signals"));
                if (signalsMap instanceof BMap) {
                    methodAction = ((BMap<BString, Object>) signalsMap).get(StringUtils.fromString(methodName));
                }

                // If not found in signals, check queries
                if (methodAction == null) {
                    Object queriesMap = modelData.get(StringUtils.fromString("queries"));
                    if (queriesMap instanceof BMap) {
                        methodAction = ((BMap<BString, Object>) queriesMap).get(StringUtils.fromString(methodName));
                    }
                }
            }

            if (!(methodAction instanceof BMap)) {
                return null;
            }

            BMap<BString, Object> actionMap = (BMap<BString, Object>) methodAction;

            // Get parameters from the WorkflowAction
            Object parameters = actionMap.get(StringUtils.fromString("parameters"));
            if (parameters instanceof BArray) {
                return (BArray) parameters;
            }
        } catch (ClassCastException e) {
            // Type casting error, return null for fallback
            // TODO: Use proper logging
        }

        return null;
    }

    /**
     * Converts a single EncodedValue based on its parameter type.
     */
    public static Object convertEncodedValueByType(EncodedValues args, int index, Object paramType) {
        try {
            // For now, implement basic type conversion based on common Ballerina types
            // TODO: Enhance this to handle complex Ballerina type descriptors

            if (paramType instanceof BString) {
                String typeName = ((BString) paramType).getValue();
                return convertByTypeName(args, index, typeName);
            }

            // Fallback to basic conversion
            return args.get(index, Object.class);

        } catch (Exception e) {
            // Fallback to basic conversion on error
            return args.get(index, Object.class);
        }
    }

    /**
     * Converts an EncodedValue based on Ballerina type name.
     */
    public static Object convertByTypeName(EncodedValues args, int index, String typeName) {
        Object value = args.get(index, Object.class);

        switch (typeName) {
            case "string":
                if (value instanceof String) {
                    return StringUtils.fromString((String) value);
                }
                break;
            case "int":
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                break;
            case "decimal":
                if (value instanceof Number) {
                    // Ballerina decimal maps to BigDecimal
                    return new java.math.BigDecimal(value.toString());
                }
                break;
            case "float":
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                break;
            case "boolean":
                if (value instanceof Boolean) {
                    return value;
                }
                break;
            // Add more type conversions as needed
            default:
                // For unknown types, return as-is
                return value;
        }

        // Fallback for type mismatches
        return value;
    }

    /**
     * Converts EncodedValues for activity execution using basic type conversion.
     * TODO: Implement type-guided conversion for activities by extracting parameter types from functions
     */
    public static Object[] convertActivityArgs(EncodedValues args) {
        Object[] result = new Object[args.getSize()];
        for (int i = 0; i < args.getSize(); i++) {
            Object value = args.get(i, Object.class);
            // Basic conversion for common types
            if (value instanceof String) {
                result[i] = StringUtils.fromString((String) value);
            } else {
                result[i] = value;
            }
        }
        return result;
    }
}
