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

public class ExecuteResult {
    @JsonProperty("effectedRows")
    private long effectedRows;
    @JsonProperty("elapsedTimeUs")
    private long elapsedTimeUs;

    public ExecuteResult() {}

    public long getEffectedRows() { return effectedRows; }
    public void setEffectedRows(long effectedRows) { this.effectedRows = effectedRows; }
    public long getElapsedTimeUs() { return elapsedTimeUs; }
    public void setElapsedTimeUs(long elapsedTimeUs) { this.elapsedTimeUs = elapsedTimeUs; }
}
