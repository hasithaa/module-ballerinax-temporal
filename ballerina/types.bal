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

public final TemporalConfig & readonly LOCAL_TEMPORAL_CONFIG = {
    serviceUrl: "localhost:7233",
    namespace: "default",
    connectionTimeout: 30000,
    rpcTimeout: 10000,
    enableTls: false
};

# Temporal server connection configuration
public type TemporalConfig record {|
    # Temporal server URL (e.g., "localhost:7233")
    string serviceUrl;
    # Temporal namespace to use (e.g., "default")
    string namespace;
    # Connection timeout in milliseconds (e.g., 30000)
    int connectionTimeout = 30000;
    # RPC timeout in milliseconds (e.g., 10000)
    int rpcTimeout = 10000;
    # Enable TLS connection
    boolean enableTls = false;
    # TLS configuration
    TlsConfig tlsConfig?;
    # Client identity for mTLS authentication
    ClientIdentity identity?;
|};

# TLS configuration for secure connections
public type TlsConfig record {|
    # Path to client certificate file
    string clientCertPath?;
    # Path to client private key file
    string clientKeyPath?;
    # Path to server root CA certificate file
    string serverRootCaPath?;
    # Server name for TLS verification
    string serverName?;
|};

# Client identity for authentication
public type ClientIdentity record {|
    # mTLS client certificate chain
    string clientCert?;
    # mTLS client private key
    string clientKey?;
|};
