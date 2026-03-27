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

public enum ValueType {
    NULL("NULL"),
    BOOL("BOOL"),
    INT("INT"),
    LONG("LONG"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    DATE("DATE"),
    DATETIME("DATETIME"),
    STRING("STRING"),
    BYTES("BYTES"),
    AS_IS("AS_IS");

    private final String value;

    ValueType(String value) {
        this.value = value;
    }

    public String getValue() { return value; }
}
