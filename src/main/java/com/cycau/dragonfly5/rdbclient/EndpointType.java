// Copyright 2025 kg.sai. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cycau.dragonfly5.rdbclient;

enum EndpointType {
    QUERY("/query"),
    EXECUTE("/execute"),
    TX_BEGIN("/tx/begin"),
    TX_QUERY("/tx/query"),
    TX_EXECUTE("/tx/execute"),
    TX_COMMIT("/tx/commit"),
    TX_ROLLBACK("/tx/rollback"),
    OTHER("/other");

    private final String path;

    EndpointType(String path) {
        this.path = path;
    }

    String getPath() { return path; }
}
