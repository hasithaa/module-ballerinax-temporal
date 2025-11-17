// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/workflow;

public isolated class TemporalPersistentProvider {

    *workflow:PersistentProvider;

    private final TemporalConfig config;
    private final map<workflow:WorkflowModelData> registeredWorkflows = {};
    private final handle temporalWorkerEngine;

    public isolated function init(TemporalConfig config) {
        // Do we need to clone the config? and Can we just forgot about it after init?
        self.config = config.clone();
        self.temporalWorkerEngine = newTemporalWorkerEngine(config);
    }

    public isolated function registerWorkflowModel(workflow:WorkflowModel svc, workflow:WorkflowModelData data) returns error? {
        lock {
            if self.registeredWorkflows.hasKey(data.workflowName) {
                return error("Workflow with name '" + data.workflowName + "' is already registered.");
            }
            self.registeredWorkflows[data.workflowName] = data.clone();
        }

        check registerService(self.temporalWorkerEngine, svc, data);
    }

    public isolated function unregisterWorkflowModel(workflow:WorkflowModel svc) returns error? {
        check unregisterWorkflowModel(self.temporalWorkerEngine, svc);
    }

    public isolated function 'start() returns error? {
        return startTemporalWorkerEngine(self.temporalWorkerEngine);
    }

    public isolated function stop() returns error? {
        return stopTemporalWorkerEngine(self.temporalWorkerEngine);
    }

    public isolated function getClient() returns workflow:WorkflowEngineClient|error {
        // Implementation to return a Temporal WorkflowEngineClient with configuration
        lock {
            return new WorkflowEngineClient();
        }
    }

    public isolated function getWorkflowOperators() returns workflow:WorkflowOperators|error {
        return error("Not implemented");
    }

}

public isolated client class WorkflowOperators {

    *workflow:WorkflowOperators;

    isolated remote function await(boolean cond) returns workflow:NotInWorkflowError|error? {
    }

    isolated remote function sleep(workflow:Duration duration) returns workflow:NotInWorkflowError|error? {
    }

};

public isolated client class WorkflowEngineClient {

    *workflow:WorkflowEngineClient;

    public isolated function init() {
    }

    isolated remote function execute(workflow:WorkflowModel svc, anydata... args) returns workflow:Execution|error {
        return error("Not implemented");
    }

    isolated remote function signal(workflow:WorkflowModel svc, workflow:Execution execution, string signalName, anydata... args) returns error? {
        return error("Not implemented");
    }

    isolated remote function query(workflow:WorkflowModel svc, workflow:Execution execution, string queryName, anydata... args) returns anydata|error {
        return error("Not implemented");
    }

    isolated remote function callActivity(function acticity, anydata... args) returns anydata|error {

    }
}
