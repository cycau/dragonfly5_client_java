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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

class Switcher {
    private static final Logger log = Logger.getLogger(Switcher.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private NodeInfo[] candidates;
    private Semaphore maxConcurrency;
    Semaphore tarConcurrency;
    private HttpClient httpClient;

    static class NodeInfo {
        String baseURL;
        String secretKey;
        String nodeId = "";
        String status = "";
        int maxHttpQueue;
        Instant checkTime;
        List<DatasourceInfo> datasources = new ArrayList<>();
        Instant nextCheckAt = Instant.EPOCH;
        final ReentrantReadWriteLock mu = new ReentrantReadWriteLock();
    }

    static class DatasourceInfo {
        String datasourceId = "";
        String databaseName = "";
        boolean active;
        int maxConns;
        int maxWriteConns;
        int minWriteConns;
    }

    String init(List<NodeEntry> entries, int maxConcurrency, int hosts) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        this.candidates = new NodeInfo[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            NodeInfo n = new NodeInfo();
            n.baseURL = entries.get(i).getBaseURL();
            n.secretKey = entries.get(i).getSecretKey();
            n.nextCheckAt = Instant.now().minusSeconds(3600);
            this.candidates[i] = n;
        }

        int maxSem = maxConcurrency * 8 / 10;
        int tarSem = maxConcurrency * 2 / 10;
        this.maxConcurrency = new Semaphore(maxSem);
        this.tarConcurrency = new Semaphore(tarSem);

        SyncResult syncResult = syncAllNodeInfo();
        return syncResult.defaultDatabase;
    }

    int getCandidateCount() {
        return candidates.length;
    }

    // Request selects a node and sends request with retry
    ResponseResult request(String traceId, String dbName, EndpointType endpoint, String method,
                           Map<String, String> headers, Map<String, Object> body, int timeoutSec,
                           int retryCount) throws HaveError {
        try {
            maxConcurrency.acquire();
        } catch (InterruptedException e) {
            throw new HaveError("ConcurrencyAcquireError", e.getMessage());
        }

        int nodeIdx;
        try {
            nodeIdx = selectNode(traceId, dbName, endpoint);
        } catch (HaveError e) {
            maxConcurrency.release();
            throw e;
        }

        NodeInfo node = candidates[nodeIdx];
        RequestResult rr = requestHttp(traceId, nodeIdx, node.baseURL, node.secretKey,
                endpoint, method, headers, body, timeoutSec, 2);
        maxConcurrency.release();

        if (rr.resp.statusCode == 200) {
            return rr.resp;
        }

        NodeInfo respNode = candidates[rr.resp.nodeIdx];
        switch (rr.resp.statusCode) {
            case 999:
                respNode.mu.writeLock().lock();
                try {
                    respNode.status = rr.error.getErrCode();
                    respNode.nextCheckAt = Instant.now().plusSeconds(15);
                } finally {
                    respNode.mu.writeLock().unlock();
                }
                break;
            case 429:
                respNode.mu.writeLock().lock();
                try {
                    respNode.status = rr.error.getErrCode();
                    respNode.nextCheckAt = Instant.now().plusSeconds(5);
                } finally {
                    respNode.mu.writeLock().unlock();
                }
                break;
            case 503:
                respNode.mu.writeLock().lock();
                try {
                    respNode.nextCheckAt = Instant.now().plusSeconds(1);
                    for (DatasourceInfo ds : respNode.datasources) {
                        if (ds.databaseName.equals(dbName)) {
                            ds.active = false;
                        }
                    }
                } finally {
                    respNode.mu.writeLock().unlock();
                }
                break;
            default:
                throw rr.error;
        }

        retryCount--;
        if (retryCount < 0) {
            log.warning(String.format("### %s Retry node: %d, endpoint: %s, retry exceeded, previous error: %s",
                    traceId, nodeIdx, endpoint, rr.error));
            throw rr.error;
        }

        try { Thread.sleep(300); } catch (InterruptedException ignored) {}

        log.info(String.format("### %s Retry node: %d, endpoint: %s, retryCounter: %d, previous error: %s",
                traceId, nodeIdx, endpoint, retryCount, rr.error));
        return request(traceId, dbName, endpoint, method, headers, body, timeoutSec, retryCount);
    }

    // RequestTargetNode sends request to a specific node with retry on network errors
    ResponseResult requestTargetNode(String traceId, int nodeIdx, EndpointType endpoint, String method,
                                     Map<String, String> headers, Map<String, Object> body,
                                     int timeoutSec, int retryCount) throws HaveError {
        NodeInfo node = candidates[nodeIdx];
        RequestResult rr = requestHttp(traceId, nodeIdx, node.baseURL, node.secretKey,
                endpoint, method, headers, body, timeoutSec, 0);

        if (rr.resp.statusCode == 200) {
            return rr.resp;
        }

        // retry only on network error
        if (rr.resp.statusCode != 999) {
            throw rr.error;
        }

        retryCount--;
        switch (retryCount) {
            case 2:
                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                break;
            case 1:
                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                break;
            case 0:
                try { Thread.sleep(2500); } catch (InterruptedException ignored) {}
                break;
            default:
                throw rr.error;
        }

        log.info(String.format("### %s Retry target node: %d, endpoint: %s, retryCounter: %d, previous error: %s",
                traceId, nodeIdx, endpoint, retryCount, rr.error));
        return requestTargetNode(traceId, nodeIdx, endpoint, method, headers, body, timeoutSec, retryCount);
    }

    private static class RequestResult {
        final ResponseResult resp;
        final HaveError error;
        RequestResult(ResponseResult resp, HaveError error) {
            this.resp = resp;
            this.error = error;
        }
    }

    private RequestResult requestHttp(String traceId, int nodeIdx, String baseURL, String secretKey,
                                      EndpointType endpoint, String method, Map<String, String> headers,
                                      Map<String, Object> body, int timeoutSec, int redirectCount) {
        try {
            String url = baseURL + "/rdb" + endpoint.getPath();

            byte[] bodyBytes = null;
            if (body != null) {
                bodyBytes = objectMapper.writeValueAsBytes(body);
            }

            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json; charset=utf-8")
                    .header("X-Trace-Id", traceId)
                    .header(RdbClient.HEADER_SECRET_KEY, secretKey)
                    .header(RdbClient.HEADER_REDIRECT_COUNT, String.valueOf(redirectCount))
                    .header(RdbClient.HEADER_TIMEOUT_SEC, String.valueOf(timeoutSec))
                    .timeout(Duration.ofSeconds(999));

            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    builder.header(entry.getKey(), entry.getValue());
                }
            }

            if ("PUT".equals(method)) {
                builder.PUT(bodyBytes != null
                        ? HttpRequest.BodyPublishers.ofByteArray(bodyBytes)
                        : HttpRequest.BodyPublishers.noBody());
            } else {
                builder.POST(bodyBytes != null
                        ? HttpRequest.BodyPublishers.ofByteArray(bodyBytes)
                        : HttpRequest.BodyPublishers.noBody());
            }

            HttpResponse<byte[]> resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());

            if (resp.statusCode() == 200) {
                String contentType = resp.headers().firstValue("Content-Type").orElse("");
                return new RequestResult(
                        new ResponseResult(200, nodeIdx, contentType, resp.body()),
                        null);
            }

            if (resp.statusCode() != 307) {
                // Error response
                try {
                    HaveError result = objectMapper.readValue(resp.body(), HaveError.class);
                    if (result.getErrCode() == null || result.getErrCode().isEmpty()) {
                        result.setErrCode("HTTP_" + resp.statusCode());
                        result.setMessage(new String(resp.body()));
                    }
                    return new RequestResult(
                            new ResponseResult(resp.statusCode(), nodeIdx, null, resp.body()),
                            result);
                } catch (Exception e) {
                    return new RequestResult(
                            new ResponseResult(-1, nodeIdx, null, null),
                            new HaveError("ResponseErrorJSONDecodingError",
                                    String.format("Status code: %d. Failed to decode error response body: %s",
                                            resp.statusCode(), e.getMessage())));
                }
            }

            // Handle 307 redirect
            redirectCount--;
            switch (redirectCount) {
                case 1:
                    try { Thread.sleep(300); } catch (InterruptedException ignored) {}
                    break;
                case 0:
                    try { Thread.sleep(900); } catch (InterruptedException ignored) {}
                    break;
                default:
                    return new RequestResult(
                            new ResponseResult(-1, nodeIdx, null, null),
                            new HaveError("RedirectCountExceeded", "Redirect count exceeded."));
            }

            String redirectNodeId = resp.headers().firstValue("Location").orElse("");
            if (redirectNodeId.isEmpty()) {
                return new RequestResult(
                        new ResponseResult(-1, nodeIdx, null, null),
                        new HaveError("RedirectLocationEmpty", "Redirect location is empty."));
            }

            // Find redirect node
            for (int i = 0; i < candidates.length; i++) {
                NodeInfo redirectNode = candidates[i];
                redirectNode.mu.readLock().lock();
                try {
                    if (redirectNode.nodeId.equals(redirectNodeId)) {
                        log.info(String.format("### %s Redirect to node %d from %d, redirectCounter: %d",
                                traceId, i, nodeIdx, redirectCount));
                        return requestHttp(traceId, i, redirectNode.baseURL, redirectNode.secretKey,
                                endpoint, method, headers, body, timeoutSec, redirectCount);
                    }
                } finally {
                    redirectNode.mu.readLock().unlock();
                }
            }

            // Sync all nodes and try again
            log.info(String.format("### %s redirect node not found, sync all nodes and try again", traceId));
            syncAllNodeInfo();

            for (int i = 0; i < candidates.length; i++) {
                NodeInfo redirectNode = candidates[i];
                redirectNode.mu.readLock().lock();
                try {
                    if (redirectNode.nodeId.equals(redirectNodeId)) {
                        log.info(String.format("### %s Redirect to node %d from %d, redirectCounter: %d",
                                traceId, i, nodeIdx, redirectCount));
                        return requestHttp(traceId, i, redirectNode.baseURL, redirectNode.secretKey,
                                endpoint, method, headers, body, timeoutSec, redirectCount);
                    }
                } finally {
                    redirectNode.mu.readLock().unlock();
                }
            }

            return new RequestResult(
                    new ResponseResult(-1, nodeIdx, null, null),
                    new HaveError("RedirectNodeNotFound",
                            String.format("Redirect node id not found. Node[%s]", redirectNodeId)));

        } catch (ConnectException | SocketTimeoutException e) {
            return new RequestResult(
                    new ResponseResult(999, nodeIdx, null, null),
                    new HaveError("NETWORK_ERROR", e.getMessage()));
        } catch (IOException e) {
            return new RequestResult(
                    new ResponseResult(999, nodeIdx, null, null),
                    new HaveError("NETWORK_ERROR", e.getMessage()));
        } catch (InterruptedException e) {
            return new RequestResult(
                    new ResponseResult(-1, nodeIdx, null, null),
                    new HaveError("HTTPRequestError", e.getMessage()));
        }
    }

    private int selectNode(String traceId, String dbName, EndpointType endpoint) throws HaveError {
        SelectResult sr = selectRandomNode(traceId, dbName, endpoint);

        if (sr.nodeIdx < 0) {
            SyncResult syncResult = syncAllNodeInfo();
            sr = selectRandomNode(traceId, dbName, endpoint);
            if (syncResult.syncCount > 0) {
                log.info(String.format("[selectNode]: sync all nodes[%d] and try again result: nodeIdx: %d, err: %s",
                        syncResult.syncCount, sr.nodeIdx, sr.error));
            }
            if (sr.nodeIdx < 0) {
                throw new HaveError("NodeSelectionError", sr.error);
            }
            return sr.nodeIdx;
        }

        // Recover problematic nodes in background
        for (int idx : sr.problematicNodes) {
            NodeInfo n = candidates[idx];
            n.mu.writeLock().lock();
            if (n.nextCheckAt.isAfter(Instant.now())) {
                n.mu.writeLock().unlock();
                continue;
            }
            n.nextCheckAt = Instant.now().plusSeconds(15);
            n.mu.writeLock().unlock();

            final int nodeIndex = idx;
            Thread.startVirtualThread(() -> {
                NodeInfo tarNode = candidates[nodeIndex];
                try {
                    NodeInfo fetched = fetchNodeInfo(tarNode.baseURL, tarNode.secretKey);
                    tarNode.mu.writeLock().lock();
                    try {
                        tarNode.nodeId = fetched.nodeId;
                        tarNode.status = fetched.status;
                        tarNode.maxHttpQueue = fetched.maxHttpQueue;
                        tarNode.datasources = fetched.datasources;
                    } finally {
                        tarNode.mu.writeLock().unlock();
                    }
                } catch (Exception e) {
                    log.warning(String.format("[selectNode] Failed to fetch node info %s: %s", tarNode.baseURL, e.getMessage()));
                    tarNode.mu.writeLock().lock();
                    try {
                        tarNode.status = "HEALZERR";
                    } finally {
                        tarNode.mu.writeLock().unlock();
                    }
                }
            });
        }

        return sr.nodeIdx;
    }

    private static class SelectResult {
        int nodeIdx = -1;
        List<Integer> problematicNodes = new ArrayList<>();
        String error;
    }

    private static class DsCandidate {
        int nodeIdx;
        int dsIdx;
        double weight;
    }

    private SelectResult selectRandomNode(String traceId, String dbName, EndpointType endpoint) {
        SelectResult result = new SelectResult();
        if (candidates == null || candidates.length == 0) {
            result.error = "Node Candidates are not initialized yet.";
            return result;
        }

        List<DsCandidate> dsCandidates = new ArrayList<>();

        for (int i = 0; i < candidates.length; i++) {
            NodeInfo n = candidates[i];
            n.mu.readLock().lock();
            try {
                if (!"SERVING".equals(n.status)) {
                    if (n.nextCheckAt.isBefore(Instant.now())) {
                        result.problematicNodes.add(i);
                    }
                    continue;
                }

                boolean problematic = false;
                for (int j = 0; j < n.datasources.size(); j++) {
                    DatasourceInfo ds = n.datasources.get(j);
                    if (!ds.databaseName.equals(dbName)) continue;
                    if (!ds.active) {
                        problematic = true;
                        continue;
                    }
                    if (endpoint == EndpointType.QUERY && (ds.maxConns - ds.minWriteConns < 1)) continue;
                    if (endpoint == EndpointType.EXECUTE && ds.maxWriteConns < 1) continue;
                    if (endpoint == EndpointType.TX_BEGIN && ds.maxWriteConns < 1) continue;

                    double weight;
                    switch (endpoint) {
                        case QUERY:
                            weight = ds.maxConns - ds.minWriteConns;
                            break;
                        case EXECUTE:
                            weight = ds.maxWriteConns;
                            break;
                        case TX_BEGIN:
                            weight = ds.maxWriteConns * 7.0 / 10.0;
                            break;
                        default:
                            weight = ds.maxConns;
                            break;
                    }
                    if (weight <= 0) continue;

                    DsCandidate dc = new DsCandidate();
                    dc.nodeIdx = i;
                    dc.dsIdx = j;
                    dc.weight = weight;
                    dsCandidates.add(dc);
                }

                if (problematic && n.nextCheckAt.isBefore(Instant.now())) {
                    result.problematicNodes.add(i);
                }
            } finally {
                n.mu.readLock().unlock();
            }
        }

        if (dsCandidates.isEmpty()) {
            log.warning(String.format("### %s [selectNode]: No available datasource for database[%s] and endpoint type[%s]",
                    traceId, dbName, endpoint));
            result.error = String.format("No available datasource for database[%s] and endpoint type[%s]", dbName, endpoint);
            return result;
        }

        double total = 0;
        for (DsCandidate c : dsCandidates) {
            total += c.weight;
        }
        double r = ThreadLocalRandom.current().nextDouble() * total;
        for (DsCandidate cand : dsCandidates) {
            r -= cand.weight;
            if (r <= 0) {
                result.nodeIdx = cand.nodeIdx;
                return result;
            }
        }

        result.nodeIdx = dsCandidates.get(dsCandidates.size() - 1).nodeIdx;
        return result;
    }

    private static class SyncResult {
        String defaultDatabase = "";
        int syncCount;
    }

    private SyncResult syncAllNodeInfo() {
        SyncResult result = new SyncResult();
        int cnt = 0;
        CountDownLatch latch = new CountDownLatch(0);
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < candidates.length; i++) {
            NodeInfo n = candidates[i];
            n.mu.writeLock().lock();
            if (n.nextCheckAt.isAfter(Instant.now())) {
                n.mu.writeLock().unlock();
                continue;
            }
            n.nextCheckAt = Instant.now().plusSeconds(15);
            n.mu.writeLock().unlock();
            cnt++;

            final NodeInfo tarNode = n;
            Thread t = Thread.startVirtualThread(() -> {
                try {
                    NodeInfo fetched = fetchNodeInfo(tarNode.baseURL, tarNode.secretKey);
                    tarNode.mu.writeLock().lock();
                    try {
                        tarNode.nodeId = fetched.nodeId;
                        tarNode.status = fetched.status;
                        tarNode.maxHttpQueue = fetched.maxHttpQueue;
                        tarNode.datasources = fetched.datasources;
                    } finally {
                        tarNode.mu.writeLock().unlock();
                    }
                } catch (Exception e) {
                    log.warning(String.format("### [Warning] Failed to fetch node info %s: %s", tarNode.baseURL, e.getMessage()));
                    tarNode.mu.writeLock().lock();
                    try {
                        tarNode.status = "HEALZERR";
                    } finally {
                        tarNode.mu.writeLock().unlock();
                    }
                }
            });
            threads.add(t);
        }

        // Wait for all threads to complete
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException ignored) {}
        }

        result.syncCount = cnt;
        if (candidates.length > 0 && !candidates[0].datasources.isEmpty()) {
            result.defaultDatabase = candidates[0].datasources.get(0).databaseName;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private NodeInfo fetchNodeInfo(String baseURL, String secretKey) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseURL + "/healz"))
                .header(RdbClient.HEADER_SECRET_KEY, secretKey)
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<byte[]> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
        Map<String, Object> map = objectMapper.readValue(resp.body(), Map.class);

        NodeInfo info = new NodeInfo();
        info.nodeId = (String) map.getOrDefault("nodeId", "");
        info.status = (String) map.getOrDefault("status", "");
        info.maxHttpQueue = map.containsKey("maxHttpQueue") ? ((Number) map.get("maxHttpQueue")).intValue() : 0;

        List<Map<String, Object>> dsList = (List<Map<String, Object>>) map.getOrDefault("datasources", List.of());
        for (Map<String, Object> dsMap : dsList) {
            DatasourceInfo ds = new DatasourceInfo();
            ds.datasourceId = (String) dsMap.getOrDefault("datasourceId", "");
            ds.databaseName = (String) dsMap.getOrDefault("databaseName", "");
            ds.active = Boolean.TRUE.equals(dsMap.get("active"));
            ds.maxConns = dsMap.containsKey("maxConns") ? ((Number) dsMap.get("maxConns")).intValue() : 0;
            ds.maxWriteConns = dsMap.containsKey("maxWriteConns") ? ((Number) dsMap.get("maxWriteConns")).intValue() : 0;
            ds.minWriteConns = dsMap.containsKey("minWriteConns") ? ((Number) dsMap.get("minWriteConns")).intValue() : 0;
            info.datasources.add(ds);
        }

        return info;
    }
}
