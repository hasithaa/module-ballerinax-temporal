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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.temporal.runtime.utils.TemporalUtils;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class WorkerEngine {

    private static final Logger logger = LoggerFactory.getLogger(WorkerEngine.class);

    // Static reference to the current worker engine instance for dispatcher access
    // TODO: Fix this. Ideally for one listener at Ballerina side, there should be one engine instance.
    private static WorkerEngine currentInstance;

    private final WorkflowServiceStubs service;
    private final WorkflowClient client;
    private final WorkerFactory factory;

    private boolean initialized = false;

    // Registry to store workflow services by name
    private final Map<String, BObject> registeredWorkflows = new HashMap<>();
    private final Map<String, BMap<BString, Object>> workflowModelData = new HashMap<>();


    private WorkerEngine() {
        this.service = WorkflowServiceStubs.newLocalServiceStubs();
        this.client = WorkflowClient.newInstance(service);
        this.factory = WorkerFactory.newInstance(client);
    }

    private WorkerEngine(WorkflowServiceStubsOptions options) {
        this.service = WorkflowServiceStubs.newServiceStubs(options);
        this.client = WorkflowClient.newInstance(service);
        this.factory = WorkerFactory.newInstance(client);
    }

    public static WorkerEngine newTemporalWorkerEngine(BMap<String, Object> config) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Temporal Worker Engine with default service stubs options.");
        }
        // TODO: Fix this. Support custom options from config map.
        WorkerEngine workerEngine = new WorkerEngine();

        // Set as current instance in a thread-safe manner
        synchronized (WorkerEngine.class) {
            currentInstance = workerEngine;
        }

        // TODO: Externalize this to accept task queue and workflow types from Ballerina side
        final String taskQueue = "default-task-queue";
        Worker worker = workerEngine.factory.newWorker(taskQueue);

        // Register the single dynamic dispatcher that handles ALL workflow types
        worker.registerWorkflowImplementationTypes(TemporalWorkflowDispatcher.class);

        // Register the dynamic activity implementation
        worker.registerActivitiesImplementations(new TemporalActivityDispatcher());
        return workerEngine;
    }

    public static Object registerService(Object engine, BObject svc, BMap<BString, Object> data) {
        if (engine instanceof WorkerEngine workerEngine) {
            // Extract workflow name from the data
            Object workflowNameObj = data.get(StringUtils.fromString("workflowName"));
            BString workflowName = workflowNameObj instanceof BString ? (BString) workflowNameObj
                    : StringUtils.fromString(workflowNameObj.toString());
            workerEngine.registerService(svc, workflowName, data);
            return null;
        } else {
            String errorMessage = "Invalid Temporal Worker Engine instance provided.";
            return TemporalUtils.createError("TemporalWorkerEngineError", errorMessage);
        }
    }

    public static Object unregisterWorkflowModel(Environment env, Object engine, BObject svc) {
        if (engine instanceof WorkerEngine workerEngine) {
            workerEngine.unregisterService(svc);
            return null;
        } else {
            String errorMessage = "Invalid Temporal Worker Engine instance provided.";
            return TemporalUtils.createError("TemporalWorkerEngineError", errorMessage);
        }
    }

    public static Object startTemporalWorkerEngine(Object engine) {
        if (engine instanceof WorkerEngine workerEngine) {
            workerEngine.start();
            return null;
        } else {
            String errorMessage = "Invalid Temporal Worker Engine instance provided.";
            return TemporalUtils.createError("TemporalWorkerEngineError", errorMessage);
        }
    }

    public static Object stopTemporalWorkerEngine(Object engine) {
        if (engine instanceof WorkerEngine workerEngine) {
            workerEngine.stop();
            return null;
        } else {
            String errorMessage = "Invalid Temporal Worker Engine instance provided.";
            return TemporalUtils.createError("TemporalWorkerEngineError", errorMessage);
        }
    }

    public synchronized void start() {

        if (!initialized) {
            if (logger.isDebugEnabled()) {
                logger.debug("Temporal Worker Engine starting...");
            }
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
            initialized = true;
            factory.start();
            if (logger.isDebugEnabled()) {
                logger.debug("Temporal Worker Engine started");
            }
        }
    }

    public synchronized void stop() {
        if (initialized) {
            if (logger.isDebugEnabled()) {
                logger.debug("Temporal Worker Engine stopping...");
            }
            factory.shutdown();
            service.shutdown();
            this.initialized = false;
            if (logger.isDebugEnabled()) {
                logger.debug("Temporal Worker Engine stopped");
            }
        }
    }

    public synchronized void registerService(BObject svc, BString workflowName, BMap<BString, Object> modelData) {
        String workflowNameStr = workflowName.getValue();
        if (logger.isDebugEnabled()) {
            logger.debug("Registering workflow service: {}", workflowNameStr);
        }

        // Store the service and model data for later use by dispatchers
        registeredWorkflows.put(workflowNameStr, svc);
        workflowModelData.put(workflowNameStr, modelData);

        if (logger.isDebugEnabled()) {
            logger.debug("Successfully registered workflow service: {}", workflowNameStr);
        }
    }

    private void unregisterService(BObject svc) {
        // Find and remove the workflow by service object
        String workflowNameToRemove = null;
        for (Map.Entry<String, BObject> entry : registeredWorkflows.entrySet()) {
            if (entry.getValue().equals(svc)) {
                workflowNameToRemove = entry.getKey();
                break;
            }
        }

        if (workflowNameToRemove != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unregistering workflow service: {}", workflowNameToRemove);
            }
            registeredWorkflows.remove(workflowNameToRemove);
            workflowModelData.remove(workflowNameToRemove);
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully unregistered workflow service: {}", workflowNameToRemove);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Workflow service not found for unregistration");
            }
        }
    }

    // TODO: Expose necessary methods to interact with the engine
    public WorkflowClient getClient() {
        return client;
    }

    /**
     * Gets the registered workflow service by name.
     *
     * @param workflowName the name of the workflow
     * @return the registered service object, or null if not found
     */
    public BObject getRegisteredWorkflow(String workflowName) {
        return registeredWorkflows.get(workflowName);
    }

    /**
     * Gets the model data for a registered workflow by name.
     *
     * @param workflowName the name of the workflow
     * @return the model data, or null if not found
     */
    public BMap<BString, Object> getWorkflowModelData(String workflowName) {
        return workflowModelData.get(workflowName);
    }

    /**
     * Gets all registered workflow names.
     *
     * @return set of registered workflow names
     */
    public java.util.Set<String> getRegisteredWorkflowNames() {
        return registeredWorkflows.keySet();
    }

    /**
     * Gets the current WorkerEngine instance.
     *
     * @return the current WorkerEngine instance, or null if not initialized
     */
    public static WorkerEngine getCurrentInstance() {
        return currentInstance;
    }
}
