/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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

package io.ballerina.stdlib.temporal.runtime.client;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.temporal.runtime.utils.TemporalUtils;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Temporal workflow client operations for Ballerina integration.
 */
public class TemporalClientManager {

    private static final Logger logger = LoggerFactory.getLogger(TemporalClientManager.class);

    // Thread-safe map to store client instances per configuration
    private static final Map<String, WorkflowClient> clients = new ConcurrentHashMap<>();
    private static final Map<String, WorkflowServiceStubs> serviceStubs = new ConcurrentHashMap<>();

    /**
     * Initialize the Temporal client with the provided configuration.
     *
     * @param env    Ballerina environment
     * @param config Temporal configuration
     * @return null if successful, error object if initialization fails
     */
    public static Object initTemporalClient(Environment env, BMap<BString, Object> config) {
        try {
            String serviceUrl = config.getStringValue(StringUtils.fromString("serviceUrl")).getValue();
            String namespace = config.getStringValue(StringUtils.fromString("namespace")).getValue();
            long connectionTimeout = (Long) config.get(StringUtils.fromString("connectionTimeout"));
            long rpcTimeout = (Long) config.get(StringUtils.fromString("rpcTimeout"));
            boolean enableTls = (Boolean) config.get(StringUtils.fromString("enableTls"));

            String clientKey = generateClientKey(serviceUrl, namespace);

            // Create workflow service stubs if not exists
            if (!serviceStubs.containsKey(clientKey)) {
                WorkflowServiceStubsOptions.Builder stubsOptionsBuilder = WorkflowServiceStubsOptions.newBuilder();
                stubsOptionsBuilder.setTarget(serviceUrl);
                stubsOptionsBuilder.setRpcTimeout(Duration.ofMillis(rpcTimeout));
                stubsOptionsBuilder.setConnectionBackoffResetFrequency(Duration.ofMillis(connectionTimeout));
                stubsOptionsBuilder.setGrpcReconnectFrequency(Duration.ofSeconds(1));

                // Add TLS configuration if enabled
                if (enableTls) {
                    // Configure TLS options based on config
                    BMap<BString, Object> tlsConfig = (BMap<BString, Object>) config.get(
                            StringUtils.fromString("tlsConfig"));
                    if (tlsConfig != null) {
                        // Configure TLS settings
                        logger.info("TLS configuration detected but not implemented in this example");
                    }
                }

                WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(stubsOptionsBuilder.build());
                serviceStubs.put(clientKey, stubs);
            }

            // Create workflow client if not exists
            if (!clients.containsKey(clientKey)) {
                DefaultDataConverter dataConverter = DefaultDataConverter.newDefaultInstance();
                WorkflowClientOptions clientOptions = WorkflowClientOptions.newBuilder().setNamespace(namespace)
                                                                           .setDataConverter(dataConverter).build();

                WorkflowClient client = WorkflowClient.newInstance(serviceStubs.get(clientKey), clientOptions);
                clients.put(clientKey, client);

                logger.info("Temporal client initialized successfully for service: {} namespace: {}", serviceUrl,
                            namespace);
            }

            return null; // Success
        } catch (Exception e) {
            logger.error("Failed to initialize Temporal client", e);
            return TemporalUtils.createError("TemporalConnectionError",
                                             "Failed to initialize Temporal client: " + e.getMessage());
        }
    }

    /**
     * Search for workflows based on process name and correlation ID.
     *
     * @param env           Ballerina environment
     * @param process       Workflow process name
     * @param correlationId Correlation identifiers for search
     * @return Workflow execution details if found, null if not found, error if search fails
     */
    public static Object searchWorkflow(Environment env, BString process, BMap<BString, Object> correlationId) {
        try {
            // For now, return null (not found) as search implementation requires more complex setup
            logger.info("Searching for workflow: {} with correlation ID: {}", process.getValue(), correlationId);
            return null;
        } catch (Exception e) {
            logger.error("Failed to search workflow", e);
            return TemporalUtils.createError("TemporalSearchError", "Failed to search workflow: " + e.getMessage());
        }
    }

    /**
     * Start a new workflow execution.
     *
     * @param env          Ballerina environment
     * @param workflowName Name of the workflow to start
     * @param methodName   Name of the workflow method to invoke
     * @param args         Arguments to pass to the workflow
     * @return Workflow execution details or error
     */
    public static Object startWorkflow(Environment env, BString workflowName, BString methodName, BArray args) {
        try {
            // Get default client (first available)
            WorkflowClient client = getDefaultClient();
            if (client == null) {
                return TemporalUtils.createError("TemporalClientError", "No Temporal client available");
            }

            // Generate unique workflow ID
            String workflowId = generateWorkflowId(workflowName.getValue());

            // Create workflow options
            WorkflowOptions options = WorkflowOptions.newBuilder().setWorkflowId(workflowId).setTaskQueue("default")
                                                     .build();

            // Create workflow stub
            WorkflowStub workflowStub = client.newUntypedWorkflowStub(workflowName.getValue(), options);

            // Convert Ballerina args to Java objects
            Object[] javaArgs = TemporalUtils.convertBArrayToJavaArray(args);

            // Start workflow execution
            workflowStub.start(javaArgs);

            logger.info("Started workflow: {} with ID: {}", workflowName.getValue(), workflowId);

            // Create execution record
            Map<String, Object> execution = new HashMap<>();
            execution.put("id", workflowId);

            return TemporalUtils.createExecutionRecord(execution);
        } catch (Exception e) {
            logger.error("Failed to start workflow", e);
            return TemporalUtils.createError("TemporalStartError", "Failed to start workflow: " + e.getMessage());
        }
    }

    /**
     * Send a signal to a running workflow.
     *
     * @param env        Ballerina environment
     * @param workflowId ID of the workflow to signal
     * @param signalName Name of the signal to send
     * @param args       Arguments for the signal
     * @return null if successful, error if signaling fails
     */
    public static Object signalWorkflow(Environment env, BString workflowId, BString signalName, BArray args) {
        try {
            WorkflowClient client = getDefaultClient();
            if (client == null) {
                return TemporalUtils.createError("TemporalClientError", "No Temporal client available");
            }

            // Get workflow stub by ID
            WorkflowStub workflowStub = client.newUntypedWorkflowStub(workflowId.getValue());

            // Convert arguments
            Object[] javaArgs = TemporalUtils.convertBArrayToJavaArray(args);

            // Send signal
            workflowStub.signal(signalName.getValue(), javaArgs);

            logger.info("Sent signal: {} to workflow: {}", signalName.getValue(), workflowId.getValue());
            return null; // Success
        } catch (Exception e) {
            logger.error("Failed to signal workflow", e);
            return TemporalUtils.createError("TemporalSignalError", "Failed to signal workflow: " + e.getMessage());
        }
    }

    /**
     * Update a running workflow.
     *
     * @param env        Ballerina environment
     * @param workflowId ID of the workflow to update
     * @param updateName Name of the update to send
     * @param args       Arguments for the update
     * @return Result of the update operation or error
     */
    public static Object updateWorkflow(Environment env, BString workflowId, BString updateName, BArray args) {
        try {
            // Workflow updates are a newer feature in Temporal - placeholder implementation
            logger.info("Update operation requested for workflow: {} update: {}", workflowId.getValue(),
                        updateName.getValue());
            return TemporalUtils.createError("TemporalUpdateError", "Workflow updates not yet implemented");
        } catch (Exception e) {
            logger.error("Failed to update workflow", e);
            return TemporalUtils.createError("TemporalUpdateError", "Failed to update workflow: " + e.getMessage());
        }
    }

    /**
     * Query a running workflow.
     *
     * @param env        Ballerina environment
     * @param workflowId ID of the workflow to query
     * @param queryName  Name of the query to execute
     * @param args       Arguments for the query
     * @return Result of the query operation or error
     */
    public static Object queryWorkflow(Environment env, BString workflowId, BString queryName, BArray args) {
        try {
            WorkflowClient client = getDefaultClient();
            if (client == null) {
                return TemporalUtils.createError("TemporalClientError", "No Temporal client available");
            }

            // Get workflow stub by ID
            WorkflowStub workflowStub = client.newUntypedWorkflowStub(workflowId.getValue());

            // Convert arguments
            Object[] javaArgs = TemporalUtils.convertBArrayToJavaArray(args);

            // Execute query
            Object result = workflowStub.query(queryName.getValue(), Object.class, javaArgs);

            logger.info("Executed query: {} on workflow: {}", queryName.getValue(), workflowId.getValue());
            return TemporalUtils.convertJavaObjectToBallerina(result);
        } catch (Exception e) {
            logger.error("Failed to query workflow", e);
            return TemporalUtils.createError("TemporalQueryError", "Failed to query workflow: " + e.getMessage());
        }
    }

    /**
     * Stop a running workflow.
     *
     * @param env        Ballerina environment
     * @param workflowId ID of the workflow to stop
     * @return null if successful, error if stopping fails
     */
    public static Object stopWorkflow(Environment env, BString workflowId) {
        try {
            WorkflowClient client = getDefaultClient();
            if (client == null) {
                return TemporalUtils.createError("TemporalClientError", "No Temporal client available");
            }

            // Get workflow stub by ID
            WorkflowStub workflowStub = client.newUntypedWorkflowStub(workflowId.getValue());

            // Cancel/terminate workflow
            workflowStub.cancel();

            logger.info("Stopped workflow: {}", workflowId.getValue());
            return null; // Success
        } catch (Exception e) {
            logger.error("Failed to stop workflow", e);
            return TemporalUtils.createError("TemporalStopError", "Failed to stop workflow: " + e.getMessage());
        }
    }

    /**
     * Generate a unique key for client identification.
     */
    private static String generateClientKey(String serviceUrl, String namespace) {
        return serviceUrl + ":" + namespace;
    }

    /**
     * Generate a unique workflow ID.
     */
    private static String generateWorkflowId(String workflowName) {
        return workflowName + "-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    }

    /**
     * Get the default (first available) client.
     */
    private static WorkflowClient getDefaultClient() {
        return clients.values().stream().findFirst().orElse(null);
    }

    /**
     * Shutdown all clients and clean up resources.
     */
    public static void shutdown() {
        logger.info("Shutting down Temporal clients");

        // Close all service stubs
        serviceStubs.values().forEach(stubs -> {
            try {
                stubs.shutdown();
            } catch (Exception e) {
                logger.warn("Error shutting down service stubs", e);
            }
        });

        // Clear all maps
        clients.clear();
        serviceStubs.clear();

        logger.info("Temporal client shutdown completed");
    }
}
