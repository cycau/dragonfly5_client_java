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

import java.util.List;
import java.util.Map;

public class Records {
    private final Map<String, Integer> colMap;
    private final List<ColumnMeta> meta;
    private final List<Record> rows;
    private final int totalCount;
    private final long elapsedTimeUs;

    Records(Map<String, Integer> colMap, List<ColumnMeta> meta, List<Record> rows, int totalCount, long elapsedTimeUs) {
        this.colMap = colMap;
        this.meta = meta;
        this.rows = rows;
        this.totalCount = totalCount;
        this.elapsedTimeUs = elapsedTimeUs;
    }

    public Record get(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return null;
        }
        return rows.get(rowIndex);
    }

    public int size() {
        return rows.size();
    }

    public List<ColumnMeta> getMeta() { return meta; }
    public int getTotalCount() { return totalCount; }
    public long getElapsedTimeUs() { return elapsedTimeUs; }
}
