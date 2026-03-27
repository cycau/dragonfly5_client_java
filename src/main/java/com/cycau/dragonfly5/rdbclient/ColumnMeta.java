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

import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnMeta {
    @JsonProperty("name")
    private String name;
    @JsonProperty("dbType")
    private String dbType;
    @JsonProperty("wireType")
    private int wireType;
    @JsonProperty("nullable")
    private boolean nullable;

    public ColumnMeta() {}

    public ColumnMeta(String name, String dbType, int wireType, boolean nullable) {
        this.name = name;
        this.dbType = dbType;
        this.wireType = wireType;
        this.nullable = nullable;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDbType() { return dbType; }
    public void setDbType(String dbType) { this.dbType = dbType; }
    public int getWireType() { return wireType; }
    public void setWireType(int wireType) { this.wireType = wireType; }
    public boolean isNullable() { return nullable; }
    public void setNullable(boolean nullable) { this.nullable = nullable; }
}
