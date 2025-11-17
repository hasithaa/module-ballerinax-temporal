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

import io.ballerina.runtime.api.values.BFunctionPointer;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.temporal.activity.Activity;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;

public class TemporalActivityDispatcher implements DynamicActivity {

    TemporalActivityDispatcher() {
    }

    @Override
    public Object execute(EncodedValues args) {
        String activityName = Activity.getExecutionContext().getInfo().getActivityType();
        return executeActivity(activityName, args);
    }

    /**
     * Execute an activity with EncodedValues.
     */
    static Object executeActivity(String activityName, EncodedValues args) {
        WorkerEngine engine = WorkerEngine.getCurrentInstance();
        if (engine == null) {
            throw new IllegalStateException("WorkerEngine not initialized");
        }

        // Get workflow type from activity execution context
        String workflowType = Activity.getExecutionContext().getInfo().getWorkflowType();
        if (workflowType == null) {
            throw new IllegalStateException("Workflow type not available in activity context");
        }

        // Get the workflow model data
        BMap<BString, Object> modelData = engine.getWorkflowModelData(workflowType);
        if (modelData == null) {
            throw new IllegalStateException("No workflow model data found for type: " + workflowType);
        }

        // Get the activity function from the activities map
        Object activitiesObj = modelData.get(io.ballerina.runtime.api.utils.StringUtils.fromString("activities"));
        if (!(activitiesObj instanceof BMap)) {
            throw new IllegalStateException("Activities not found in workflow model data");
        }

        BMap<BString, Object> activities = (BMap<BString, Object>) activitiesObj;
        Object activityFunction = activities.get(io.ballerina.runtime.api.utils.StringUtils.fromString(activityName));
        if (!(activityFunction instanceof BFunctionPointer)) {
            throw new IllegalStateException("Activity function not found: " + activityName);
        }

        BFunctionPointer func = (BFunctionPointer) activityFunction;

        // Convert EncodedValues to Ballerina types
        // For activities, we need to extract parameter types from the function
        // This is more complex than workflow methods, so for now use basic conversion
        Object[] argsArray = TypeConversionUtils.convertActivityArgs(args);

        return BRuntimeHandler.executeActivity(func, argsArray);
    }
}
