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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.util.*;
import java.util.logging.Logger;

public class RdbClient {
    private static final Logger log = Logger.getLogger(RdbClient.class.getName());

    static final String HEADER_SECRET_KEY = "_cy_SecretKey";
    static final String HEADER_DB_NAME = "_cy_DbName";
    static final String HEADER_TX_ID = "_cy_TxID";
    static final String HEADER_REDIRECT_COUNT = "_cy_RdCount";
    static final String HEADER_TIMEOUT_SEC = "_cy_TimeoutSec";

    private static final String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final Random random = new Random();

    static final Switcher executor = new Switcher();
    private static String defaultDatabase = "";

    private final String dbName;
    private final String traceId;

    private RdbClient(String dbName, String traceId) {
        this.dbName = dbName;
        this.traceId = traceId;
    }

    // ============ Initialization ============

    public static void init(List<NodeEntry> nodes, String defDatabase, int maxConcurrency) throws Exception {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("No cluster nodes configured.");
        }
        maxConcurrency = Math.max(10, maxConcurrency);

        log.info("### [Init] maxConcurrency: " + maxConcurrency);
        log.info("### [Init] entries: " + nodes.size());

        String foundDatabase = executor.init(nodes, maxConcurrency, nodes.size());
        if (foundDatabase == null || foundDatabase.isEmpty()) {
            throw new RuntimeException("Failed to get datasource info from cluster nodes.");
        }
        defaultDatabase = (defDatabase != null && !defDatabase.isEmpty()) ? defDatabase : foundDatabase;

        log.info("### [Init] defaultDatabase: " + defaultDatabase);
    }

    @SuppressWarnings("unchecked")
    public static void initWithYamlFile(String filePath) throws Exception {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        Map<String, Object> config = yamlMapper.readValue(new File(filePath), Map.class);

        int maxConcurrency = config.containsKey("maxConcurrency")
                ? ((Number) config.get("maxConcurrency")).intValue() : 100;
        String defaultSecretKey = (String) config.getOrDefault("defaultSecretKey", "");
        String defDatabase = (String) config.getOrDefault("defaultDatabase", "");

        List<Map<String, String>> nodesList = (List<Map<String, String>>) config.getOrDefault("clusterNodes", List.of());
        List<NodeEntry> nodes = new ArrayList<>();
        for (Map<String, String> nodeMap : nodesList) {
            String baseUrl = nodeMap.getOrDefault("baseUrl", "");
            String secretKey = nodeMap.getOrDefault("secretKey", "");
            if (secretKey.isEmpty()) {
                secretKey = defaultSecretKey;
            }
            nodes.add(new NodeEntry(baseUrl, secretKey));
        }

        init(nodes, defDatabase, maxConcurrency);
    }

    // ============ Client Factory ============

    public static RdbClient getDefault() {
        return get("", "");
    }

    public static RdbClient get(String databaseName, String traceId) {
        if (databaseName == null || databaseName.isEmpty()) {
            databaseName = defaultDatabase;
        }
        if (traceId == null || traceId.isEmpty()) {
            traceId = "d5" + generateNanoId(10);
        }
        return new RdbClient(databaseName, traceId);
    }

    // ============ Query / Execute ============

    public Records query(String sql, Params params, QueryOptions opts) throws HaveError {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_DB_NAME, dbName);

        List<Map<String, Object>> paramValues = (params != null) ? params.getData() : List.of();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sql", sql);
        body.put("params", paramValues);

        int timeoutSec = 0;
        if (opts != null) {
            if (opts.getOffsetRows() > 0) body.put("offsetRows", opts.getOffsetRows());
            if (opts.getLimitRows() > 0) body.put("limitRows", opts.getLimitRows());
            timeoutSec = opts.getTimeoutSec();
        }

        ResponseResult resp = executor.request(traceId, dbName, EndpointType.QUERY,
                "POST", headers, body, timeoutSec, 2);
        return QueryResultParser.parseQueryResult(resp);
    }

    public ExecuteResult execute(String sql, Params params) throws HaveError {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_DB_NAME, dbName);

        List<Map<String, Object>> paramValues = (params != null) ? params.getData() : List.of();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sql", sql);
        body.put("params", paramValues);

        ResponseResult resp = executor.request(traceId, dbName, EndpointType.EXECUTE,
                "POST", headers, body, 0, 2);

        try {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(resp.body, ExecuteResult.class);
        } catch (Exception e) {
            throw new HaveError("ResponseBodyJSONDecodeError", e.getMessage());
        }
    }

    // ============ Utilities ============

    static String getDefaultDatabase() {
        return defaultDatabase;
    }

    static String generateNanoId(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append(ALPHA_NUM.charAt(random.nextInt(ALPHA_NUM.length())));
        }
        return sb.toString();
    }
}
