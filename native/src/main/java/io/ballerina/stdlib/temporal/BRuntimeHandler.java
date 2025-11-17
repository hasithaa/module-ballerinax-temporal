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

import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.values.BFunctionPointer;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.temporal.runtime.utils.ModuleUtils;

import java.util.HashMap;

public class BRuntimeHandler {

    // TODO: Move this to Ballerina/Workflow Module in M2. (Temporary placed here to avoid cyclic dependency issues)

    public static Object executeWorkflow(BObject svc, String methodName, Object... args) {
        final Runtime runtime = ModuleUtils.getRuntime();
        return runtime.callMethod(svc, methodName, createStrandMetadata(), args);
    }

    public static Object executeActivity(Module module, String functionName, Object... args) {
        final Runtime runtime = ModuleUtils.getRuntime();
        return runtime.callFunction(module, functionName, createStrandMetadata(), args);
    }

    public static Object executeActivity(BFunctionPointer func, Object... args) {
        final Runtime runtime = ModuleUtils.getRuntime();
        return func.call(runtime, args);
    }

    public static StrandMetadata createStrandMetadata() {
        // TODO: Attache Persistent Provider
        HashMap<String, Object> data = new HashMap<>();
        return new StrandMetadata(false, data);
    }
}
