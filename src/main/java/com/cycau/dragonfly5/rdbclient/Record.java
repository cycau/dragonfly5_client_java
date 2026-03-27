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

import java.util.Map;

public class Record {
    private final Map<String, Integer> colMap;
    private final Object[] data;

    Record(Map<String, Integer> colMap, Object[] data) {
        this.colMap = colMap;
        this.data = data;
    }

    public Object get(String columnName) {
        Integer idx = colMap.get(columnName);
        if (idx == null) {
            throw new IllegalArgumentException("Column name " + columnName + " not exist");
        }
        return data[idx];
    }

    public Object getByIdx(int idx) {
        return data[idx];
    }

    @SuppressWarnings("unchecked")
    public <T> T getAs(String columnName, Class<T> type) {
        Object val = get(columnName);
        if (val == null) return null;
        return (T) val;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAsByIdx(int idx, Class<T> type) {
        Object val = getByIdx(idx);
        if (val == null) return null;
        return (T) val;
    }
}
