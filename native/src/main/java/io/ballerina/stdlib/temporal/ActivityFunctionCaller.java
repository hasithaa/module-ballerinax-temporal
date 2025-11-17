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

import io.temporal.activity.Activity;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatcher that executes activity implementations dynamically.
 * <p>
 * This implementation inspects the Temporal activity execution context to determine the
 * activity type (name) and decodes incoming arguments using Temporal's {@link EncodedValues}.
 * The implementation currently has placeholders for converting inputs to Ballerina values and
 * invoking the corresponding Ballerina function.
 *
 * @since 0.1.0
 */
public class ActivityFunctionCaller implements DynamicActivity {

    private static final Logger logger = LoggerFactory.getLogger(ActivityFunctionCaller.class);

    /**
     * Execute the activity with the provided encoded arguments.
     *
     * @param encodedValues encoded input arguments supplied by the Temporal runtime
     * @return the activity result, or {@code null} when result conversion/invocation is not implemented
     */
    @Override
    public Object execute(EncodedValues encodedValues) {

        // Get the activity name from Temporal's execution context
        String activityName = Activity.getExecutionContext().getInfo().getActivityType();
        if (logger.isDebugEnabled()) {
            logger.debug("Invoking activity: {}", activityName);
        }

        // TODO: (TEMPORAL-001) - Map activity name to Ballerina function and get the type information

        // TODO: (TEMPORAL-002) - Get all input arguments and convert to Ballerina compatible values

        // TODO: (TEMPORAL-003) - Invoke Ballerina function based on activity name and input args

        return null;
    }
}
