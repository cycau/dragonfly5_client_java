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

import java.util.*;
import java.util.logging.Logger;

public class TxClient implements AutoCloseable {
    private static final Logger log = Logger.getLogger(TxClient.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String dbName;
    private final Switcher executor;
    private final int nodeIdx;
    private final String orgTxId;
    private final String traceId;
    private boolean isClosed;

    private TxClient(String dbName, Switcher executor, int nodeIdx, String orgTxId, String traceId) {
        this.dbName = dbName;
        this.executor = executor;
        this.nodeIdx = nodeIdx;
        this.orgTxId = orgTxId;
        this.traceId = traceId;
        this.isClosed = false;
    }

    // ============ Factory ============

    public static TxClient newTxDefault() throws HaveError {
        return newTx("", null, 0, "");
    }

    public static TxClient newTx(String databaseName, IsolationLevel isolationLevel,
                                  int maxTxTimeoutSec, String traceId) throws HaveError {
        if (databaseName == null || databaseName.isEmpty()) {
            databaseName = RdbClient.getDefaultDatabase();
        }
        if (traceId == null || traceId.isEmpty()) {
            traceId = "d5" + RdbClient.generateNanoId(10);
        }

        BeginResult br = beginTx(traceId, databaseName, isolationLevel, maxTxTimeoutSec);
        return new TxClient(databaseName, RdbClient.executor, br.nodeIdx, br.txId, traceId);
    }

    public static TxClient restoreTx(String txId) throws Exception {
        String[] parts = txId.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("invalid txId format");
        }
        int nodeIdx = Integer.parseInt(parts[1]);
        if (nodeIdx < 0 || nodeIdx >= RdbClient.executor.getCandidateCount()) {
            throw new IllegalArgumentException("invalid node index in txId");
        }
        return new TxClient("", RdbClient.executor, nodeIdx, parts[0], parts[2]);
    }

    // ============ Transaction ID ============

    public String getTxId() {
        return orgTxId + "." + nodeIdx + "." + traceId;
    }

    // ============ Query / Execute ============

    public Records query(String sql, Params params, QueryOptions opts) throws HaveError {
        if (isClosed) {
            throw new HaveError("TxClosedError", "Transaction is already closed");
        }

        Map<String, String> headers = new HashMap<>();
        headers.put(RdbClient.HEADER_TX_ID, orgTxId);

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

        ResponseResult resp = executor.requestTargetNode(traceId, nodeIdx,
                EndpointType.TX_QUERY, "POST", headers, body, timeoutSec, 2);
        return QueryResultParser.parseQueryResult(resp);
    }

    public ExecuteResult execute(String sql, Params params) throws HaveError {
        if (isClosed) {
            throw new HaveError("TxClosedError", "Transaction is already closed");
        }

        Map<String, String> headers = new HashMap<>();
        headers.put(RdbClient.HEADER_TX_ID, orgTxId);

        List<Map<String, Object>> paramValues = (params != null) ? params.getData() : List.of();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sql", sql);
        body.put("params", paramValues);

        ResponseResult resp = executor.requestTargetNode(traceId, nodeIdx,
                EndpointType.TX_EXECUTE, "POST", headers, body, 0, 2);

        try {
            return objectMapper.readValue(resp.body, ExecuteResult.class);
        } catch (Exception e) {
            throw new HaveError("ResponseBodyJSONDecodeError", e.getMessage());
        }
    }

    // ============ BulkInsert ============

    private static final int MAX_PARAMS_PER_CHUNK = 20_000;

    public ExecuteResult bulkInsert(String tableName, String[] columnNames, List<Params> records) throws HaveError {
        if (isClosed) {
            throw new HaveError("TxClosedError", "Transaction is already closed");
        }
        if (columnNames == null || columnNames.length == 0) {
            throw new HaveError("InvalidArgument", "columnNames must not be empty");
        }
        if (records == null || records.isEmpty()) {
            throw new HaveError("InvalidArgument", "records must not be empty");
        }

        int colCount = columnNames.length;

        // Validate all records have correct param count
        for (int i = 0; i < records.size(); i++) {
            if (records.get(i).getData().size() != colCount) {
                throw new HaveError("InvalidArgument",
                        String.format("record[%d] has %d params, expected %d",
                                i, records.get(i).getData().size(), colCount));
            }
        }

        // Build SQL prefix and row placeholder once
        StringBuilder prefixBuilder = new StringBuilder("INSERT INTO ");
        prefixBuilder.append(tableName).append(" (");
        for (int i = 0; i < colCount; i++) {
            if (i > 0) prefixBuilder.append(", ");
            prefixBuilder.append(columnNames[i]);
        }
        prefixBuilder.append(") VALUES ");
        String sqlPrefix = prefixBuilder.toString();

        StringBuilder phBuilder = new StringBuilder("(");
        for (int i = 0; i < colCount; i++) {
            if (i > 0) phBuilder.append(", ");
            phBuilder.append("?");
        }
        phBuilder.append(")");
        String rowPlaceholder = phBuilder.toString();

        // Chunk size
        int rowsPerChunk = Math.max(1, MAX_PARAMS_PER_CHUNK / colCount);

        long totalEffected = 0;
        long totalElapsed = 0;

        int totalRecords = records.size();
        log.info(String.format("### %s BulkInsert: totalRecords=%d", traceId, totalRecords));
        for (int offset = 0; offset < totalRecords; offset += rowsPerChunk) {
            int end = Math.min(offset + rowsPerChunk, totalRecords);
            List<Params> chunk = records.subList(offset, end);

            // Build SQL
            StringBuilder sb = new StringBuilder(sqlPrefix);
            List<Map<String, Object>> allParams = new ArrayList<>(colCount * chunk.size());
            for (int i = 0; i < chunk.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(rowPlaceholder);
                allParams.addAll(chunk.get(i).getData());
            }

            ExecuteResult result = executeChunk(sb.toString(), allParams);
            totalEffected += result.getEffectedRows();
            totalElapsed += result.getElapsedTimeUs();

            log.info(String.format("### %s BulkInsert in progress: effectedRows=%d, elapsedTimeMs=%d",
                    traceId, totalEffected, totalElapsed / 1000));
        }

        ExecuteResult total = new ExecuteResult();
        total.setEffectedRows(totalEffected);
        total.setElapsedTimeUs(totalElapsed);
        return total;
    }

    private ExecuteResult executeChunk(String sql, List<Map<String, Object>> params) throws HaveError {
        Map<String, String> headers = new HashMap<>();
        headers.put(RdbClient.HEADER_TX_ID, orgTxId);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sql", sql);
        body.put("params", params);

        ResponseResult resp = executor.requestTargetNode(traceId, nodeIdx,
                EndpointType.TX_EXECUTE, "POST", headers, body, 0, 2);

        try {
            return objectMapper.readValue(resp.body, ExecuteResult.class);
        } catch (Exception e) {
            throw new HaveError("ResponseBodyJSONDecodeError", e.getMessage());
        }
    }

    // ============ Commit / Rollback / Release ============

    public void commit() throws HaveError {
        if (isClosed) {
            throw new HaveError("TxClosedError", "Transaction is already closed");
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(RdbClient.HEADER_TX_ID, orgTxId);

        executor.requestTargetNode(traceId, nodeIdx,
                EndpointType.TX_COMMIT, "PUT", headers, null, 0, 2);
        isClosed = true;
    }

    public void rollback() throws HaveError {
        if (isClosed) {
            throw new HaveError("TxClosedError", "Transaction is already closed");
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(RdbClient.HEADER_TX_ID, orgTxId);

        executor.requestTargetNode(traceId, nodeIdx,
                EndpointType.TX_ROLLBACK, "PUT", headers, null, 0, 2);
        isClosed = true;
    }

    public void close() {
        if (isClosed) return;
        Map<String, String> headers = new HashMap<>();
        headers.put(RdbClient.HEADER_TX_ID, orgTxId);

        try {
            executor.requestTargetNode(traceId, nodeIdx,
                    EndpointType.TX_ROLLBACK, "PUT", headers, null, 0, 3);
            isClosed = true;
        } catch (HaveError e) {
            log.warning(String.format("### TxClient Close Error: traceId=%s, txId=%s, nodeIdx=%d, error=%s",
                    traceId, orgTxId, nodeIdx, e));
        }
    }

    // ============ Internal: Begin Transaction ============

    private static class BeginResult {
        String txId;
        int nodeIdx;
    }

    private static BeginResult beginTx(String traceId, String databaseName,
                                        IsolationLevel isolationLevel, int maxTxTimeoutSec) throws HaveError {
        try {
            RdbClient.executor.tarConcurrency.acquire();
        } catch (InterruptedException e) {
            throw new HaveError("TxConcurrencyAcquireError", e.getMessage());
        }

        try {
            Map<String, String> headers = new HashMap<>();
            headers.put(RdbClient.HEADER_DB_NAME, databaseName);

            Map<String, Object> body = new LinkedHashMap<>();
            if (isolationLevel != null) {
                body.put("isolationLevel", isolationLevel.getValue());
            }
            if (maxTxTimeoutSec > 0) {
                body.put("maxTxTimeoutSec", maxTxTimeoutSec);
            }

            ResponseResult resp = RdbClient.executor.request(traceId, databaseName,
                    EndpointType.TX_BEGIN, "POST", headers, body, 0, 2);

            try {
                TxInfo result = objectMapper.readValue(resp.body, TxInfo.class);
                BeginResult br = new BeginResult();
                br.txId = result.getTxId();
                br.nodeIdx = resp.nodeIdx;
                return br;
            } catch (Exception e) {
                throw new HaveError("ResponseBodyJSONDecodeError", e.getMessage());
            }
        } finally {
            RdbClient.executor.tarConcurrency.release();
        }
    }
}
