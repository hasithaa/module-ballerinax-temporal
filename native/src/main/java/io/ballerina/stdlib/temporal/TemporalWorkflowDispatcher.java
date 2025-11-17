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

import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.temporal.common.converter.EncodedValues;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInfo;

public class TemporalWorkflowDispatcher implements DynamicWorkflow {

    public TemporalWorkflowDispatcher() {
        Workflow.registerListener((io.temporal.workflow.DynamicSignalHandler) this::handleSignal);
        Workflow.registerListener((io.temporal.workflow.DynamicQueryHandler) this::handleQuery);
    }

    @Override
    public Object execute(EncodedValues args) {

        WorkflowInfo info = Workflow.getInfo();
        String workflowType = info.getWorkflowType();

        // Access workflow Engine and identify the workflow from the workflowType
        WorkerEngine engine = WorkerEngine.getCurrentInstance();
        if (engine == null) {
            throw new IllegalStateException("WorkerEngine not initialized");
        }

        BObject svc = engine.getRegisteredWorkflow(workflowType);
        if (svc == null) {
            throw new IllegalStateException("No workflow service registered for type: " + workflowType);
        }

        // Convert EncodedValues args to Ballerina values
        Object[] argsArray = TypeConversionUtils.convertEncodedValuesToBallerina(engine, workflowType, "execute", args);

        return BRuntimeHandler.executeWorkflow(svc, "execute", argsArray);
    }

    private Object handleSignal(String signalName, EncodedValues args) {
        WorkflowInfo info = Workflow.getInfo();
        String workflowType = info.getWorkflowType();

        WorkerEngine engine = WorkerEngine.getCurrentInstance();
        if (engine == null) {
            throw new IllegalStateException("WorkerEngine not initialized");
        }

        BObject svc = engine.getRegisteredWorkflow(workflowType);
        if (svc == null) {
            throw new IllegalStateException("No workflow service registered for type: " + workflowType);
        }

        Object[] argsArray = TypeConversionUtils.convertEncodedValuesToBallerina(engine, workflowType,
                                                                                   signalName, args);

        return BRuntimeHandler.executeWorkflow(svc, signalName, argsArray);
    }

    private Object handleQuery(String queryName, EncodedValues args) {
        WorkflowInfo info = Workflow.getInfo();
        String workflowType = info.getWorkflowType();

        WorkerEngine engine = WorkerEngine.getCurrentInstance();
        if (engine == null) {
            throw new IllegalStateException("WorkerEngine not initialized");
        }

        BObject svc = engine.getRegisteredWorkflow(workflowType);
        if (svc == null) {
            throw new IllegalStateException("No workflow service registered for type: " + workflowType);
        }

        Object[] argsArray = TypeConversionUtils.convertEncodedValuesToBallerina(engine, workflowType, queryName, args);

        return BRuntimeHandler.executeWorkflow(svc, queryName, argsArray);
    }

    /**
     * Converts EncodedValues to Ballerina-compatible objects based on workflow model data.
     * Uses parameter type information from the workflow definition to perform type-safe conversion.
     */
    private Object[] convertEncodedValuesToBallerina(WorkerEngine engine, String workflowType,
                                                   String methodName, EncodedValues args) {
        return TypeConversionUtils.convertEncodedValuesToBallerina(engine, workflowType, methodName, args);
    }

    /**
     * Extracts method parameters from workflow model data.
     */
    private BArray extractMethodParameters(BMap<BString, Object> modelData, String methodName) {
        return TypeConversionUtils.extractMethodParameters(modelData, methodName);
    }

    /**
     * Converts a single EncodedValue based on its parameter type.
     */
    private Object convertEncodedValueByType(EncodedValues args, int index, Object paramType) {
        return TypeConversionUtils.convertEncodedValueByType(args, index, paramType);
    }

    /**
     * Converts an EncodedValue based on Ballerina type name.
     */
    private Object convertByTypeName(EncodedValues args, int index, String typeName) {
        return TypeConversionUtils.convertByTypeName(args, index, typeName);
    }
}
