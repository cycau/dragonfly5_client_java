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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

class QueryResultParser {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false);

    // Binary protocol constants
    static final byte BIN_PROTO_VERSION = 0x04;
    static final int BIN_END_OF_ROWS = 0x00000000;
    static final byte BIN_TRAILER_ERROR = 0x01;

    // Wire type constants
    static final byte WIRE_NULL = 0x00;
    static final byte WIRE_BOOL1 = 0x01;
    static final byte WIRE_BOOL0 = 0x02;
    static final byte WIRE_INT8 = 0x03;
    static final byte WIRE_INT16 = 0x04;
    static final byte WIRE_INT32 = 0x05;
    static final byte WIRE_INT64 = 0x06;
    static final byte WIRE_UINT8 = 0x07;
    static final byte WIRE_UINT16 = 0x08;
    static final byte WIRE_UINT32 = 0x09;
    static final byte WIRE_UINT64 = 0x0A;
    static final byte WIRE_FLOAT32 = 0x0B;
    static final byte WIRE_FLOAT64 = 0x0C;
    static final byte WIRE_DATETIME = 0x0D;
    static final byte WIRE_STRING = 0x0E;
    static final byte WIRE_BYTES = 0x0F;

    static Records parseQueryResult(ResponseResult resp) throws HaveError {
        if (resp.contentType != null && resp.contentType.contains("octet")) {
            return parseBinaryQueryResult(resp.body);
        } else if (resp.contentType != null && resp.contentType.contains("json")) {
            return parseJsonQueryResult(resp.body);
        } else {
            throw new HaveError("UnsupportedContentType",
                    "unsupported content type: " + resp.contentType);
        }
    }

    // ============ JSON Protocol ============

    private static Records parseJsonQueryResult(byte[] data) throws HaveError {
        try {
            JsonNode root = objectMapper.readTree(data);

            // Parse meta
            List<ColumnMeta> meta = new ArrayList<>();
            JsonNode metaNode = root.get("meta");
            if (metaNode != null && metaNode.isArray()) {
                for (JsonNode m : metaNode) {
                    ColumnMeta cm = new ColumnMeta(
                            m.has("name") ? m.get("name").asText() : "",
                            m.has("dbType") ? m.get("dbType").asText() : "",
                            m.has("wireType") ? m.get("wireType").asInt() : 0,
                            m.has("nullable") && m.get("nullable").asBoolean()
                    );
                    meta.add(cm);
                }
            }

            int totalCount = root.has("totalCount") ? root.get("totalCount").asInt() : 0;
            long elapsedTimeUs = root.has("elapsedTimeUs") ? root.get("elapsedTimeUs").asLong() : 0;

            // Parse rows with wire type restoration
            List<Object[]> rows = new ArrayList<>();
            JsonNode rowsNode = root.get("rows");
            if (rowsNode != null && rowsNode.isArray()) {
                for (JsonNode rowNode : rowsNode) {
                    Object[] row = new Object[meta.size()];
                    for (int j = 0; j < meta.size() && j < rowNode.size(); j++) {
                        JsonNode cell = rowNode.get(j);
                        if (cell == null || cell.isNull()) {
                            row[j] = null;
                        } else {
                            row[j] = restoreJsonType(cell, (byte) meta.get(j).getWireType());
                        }
                    }
                    rows.add(row);
                }
            }

            return makeRecords(meta, rows, totalCount, elapsedTimeUs);
        } catch (HaveError e) {
            throw e;
        } catch (Exception e) {
            throw new HaveError("ResponseBodyJSONDecodeError", e.getMessage());
        }
    }

    private static Object restoreJsonType(JsonNode val, byte wireType) throws HaveError {
        switch (wireType) {
            case WIRE_NULL:
                return null;
            case WIRE_BOOL0:
            case WIRE_BOOL1:
                if (val.isBoolean()) return val.asBoolean();
                if (val.isTextual()) {
                    String s = val.asText();
                    if ("true".equalsIgnoreCase(s) || "1".equals(s)) return true;
                    if ("false".equalsIgnoreCase(s) || "0".equals(s)) return false;
                }
                throw new HaveError("RestoreJsonTypeError", "Invalid bool value: " + val);
            case WIRE_INT8:
            case WIRE_INT16:
            case WIRE_INT32:
                if (val.isNumber()) return val.asInt();
                if (val.isTextual()) return Integer.parseInt(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid int32 value: " + val);
            case WIRE_INT64:
                if (val.isNumber()) return val.asLong();
                if (val.isTextual()) return Long.parseLong(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid int64 value: " + val);
            case WIRE_UINT8:
            case WIRE_UINT16:
            case WIRE_UINT32:
                if (val.isNumber()) return val.asInt() & 0xFFFFFFFFL;
                if (val.isTextual()) return Long.parseLong(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid uint32 value: " + val);
            case WIRE_UINT64:
                if (val.isNumber()) return val.asLong();
                if (val.isTextual()) return Long.parseUnsignedLong(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid uint64 value: " + val);
            case WIRE_FLOAT32:
                if (val.isNumber()) return val.floatValue();
                if (val.isTextual()) return Float.parseFloat(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid float32 value: " + val);
            case WIRE_FLOAT64:
                if (val.isNumber()) return val.doubleValue();
                if (val.isTextual()) return Double.parseDouble(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid float64 value: " + val);
            case WIRE_DATETIME:
                if (val.isTextual()) return parseDateTime(val.asText());
                if (val.isNumber()) return Instant.ofEpochMilli(val.asLong() / 1_000_000);
                throw new HaveError("RestoreJsonTypeError", "Invalid datetime value: " + val);
            case WIRE_STRING:
                return val.asText();
            case WIRE_BYTES:
                if (val.isTextual()) return Base64.getDecoder().decode(val.asText());
                throw new HaveError("RestoreJsonTypeError", "Invalid bytes value: " + val);
            default:
                throw new HaveError("RestoreJsonTypeError",
                        String.format("unknown wire type: 0x%02x", wireType));
        }
    }

    // ============ Binary Protocol v4 ============

    private static Records parseBinaryQueryResult(byte[] data) throws HaveError {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

        // Section 1: Header + Column Metadata
        byte version = buf.get();
        if (version != BIN_PROTO_VERSION) {
            throw new HaveError("BinaryProtocolError", "invalid protocol version");
        }

        int metaCount = buf.getShort() & 0xFFFF;
        List<ColumnMeta> meta = new ArrayList<>(metaCount);
        for (int i = 0; i < metaCount; i++) {
            int nameLen = buf.getShort() & 0xFFFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);

            int dbTypeLen = buf.getShort() & 0xFFFF;
            byte[] dbTypeBytes = new byte[dbTypeLen];
            buf.get(dbTypeBytes);

            byte nullable = buf.get();
            meta.add(new ColumnMeta(new String(nameBytes), new String(dbTypeBytes), 0, nullable != 0));
        }

        // Section 2: Row Data
        List<Object[]> rows = new ArrayList<>();
        while (true) {
            int rowLen = buf.getInt();
            if (rowLen == BIN_END_OF_ROWS) break;

            byte[] rowBytes = new byte[rowLen];
            buf.get(rowBytes);
            Object[] row = parseBinaryRow(rowBytes, metaCount);
            rows.add(row);
        }

        // Section 3: Trailer
        byte trailerType = buf.get();
        if (trailerType == BIN_TRAILER_ERROR) {
            int msgLen = buf.getShort() & 0xFFFF;
            byte[] msgBytes = new byte[msgLen];
            buf.get(msgBytes);
            throw new HaveError("ServerStreamError", new String(msgBytes));
        }

        long totalCount = buf.getLong();
        long elapsedUs = buf.getLong();

        return makeRecords(meta, rows, (int) totalCount, elapsedUs);
    }

    private static Object[] parseBinaryRow(byte[] rowBytes, int columnCount) throws HaveError {
        ByteBuffer buf = ByteBuffer.wrap(rowBytes).order(ByteOrder.BIG_ENDIAN);
        Object[] row = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            byte wireType = buf.get();
            row[i] = parseBinaryColumn(buf, wireType);
        }
        return row;
    }

    private static Object parseBinaryColumn(ByteBuffer buf, byte wireType) throws HaveError {
        switch (wireType) {
            case WIRE_NULL: return null;
            case WIRE_BOOL0: return false;
            case WIRE_BOOL1: return true;
            case WIRE_INT8: return (int) buf.get();
            case WIRE_INT16: return (int) buf.getShort();
            case WIRE_INT32: return buf.getInt();
            case WIRE_INT64: return buf.getLong();
            case WIRE_UINT8: return (long) (buf.get() & 0xFF);
            case WIRE_UINT16: return (long) (buf.getShort() & 0xFFFF);
            case WIRE_UINT32: return (long) (buf.getInt() & 0xFFFFFFFFL);
            case WIRE_UINT64: return buf.getLong();
            case WIRE_FLOAT32: return buf.getFloat();
            case WIRE_FLOAT64: return buf.getDouble();
            case WIRE_DATETIME: {
                int valueLen = buf.get() & 0xFF;
                byte[] b = new byte[valueLen];
                buf.get(b);
                return parseDateTime(new String(b));
            }
            case WIRE_STRING: {
                int valueLen = buf.getInt();
                byte[] b = new byte[valueLen];
                buf.get(b);
                return new String(b);
            }
            case WIRE_BYTES: {
                int valueLen = buf.getInt();
                byte[] b = new byte[valueLen];
                buf.get(b);
                return b;
            }
            default:
                throw new HaveError("BinaryProtocolError",
                        String.format("unknown wire type: 0x%02x", wireType));
        }
    }

    // ============ DB Type Restoration & Records Building ============

    private static Records makeRecords(List<ColumnMeta> meta, List<Object[]> rawRows,
                                       int totalCount, long elapsedTimeUs) throws HaveError {
        Map<String, Integer> colMap = new HashMap<>();
        for (int i = 0; i < meta.size(); i++) {
            colMap.put(meta.get(i).getName(), i);
        }

        List<Record> records = new ArrayList<>(rawRows.size());
        for (int i = 0; i < rawRows.size(); i++) {
            Object[] row = rawRows.get(i);
            for (int j = 0; j < row.length; j++) {
                if (row[j] == null) continue;
                row[j] = restoreDbType(row[j], meta.get(j).getDbType());
            }
            records.add(new Record(colMap, row));
        }

        return new Records(colMap, meta, records, totalCount, elapsedTimeUs);
    }

    private static Object restoreDbType(Object val, String dbType) throws HaveError {
        if (val == null) return null;
        String upper = dbType.toUpperCase();

        switch (upper) {
            case "BOOL":
            case "BOOLEAN":
                if (val instanceof Boolean) return val;
                if (val instanceof String s) {
                    if ("true".equalsIgnoreCase(s) || "1".equals(s)) return true;
                    if ("false".equalsIgnoreCase(s) || "0".equals(s)) return false;
                }
                throw new HaveError("InvalidType", "Invalid bool value: " + val);

            case "INT":
            case "INT2":
            case "INT4":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
                if (val instanceof Integer) return val;
                if (val instanceof Long l) return l.intValue();
                if (val instanceof Number n) return n.intValue();
                if (val instanceof String s) return Integer.parseInt(s);
                return Integer.parseInt(String.valueOf(val));

            case "LONG":
            case "INT8":
            case "BIGINT":
                if (val instanceof Long) return val;
                if (val instanceof Integer i) return (long) i;
                if (val instanceof Number n) return n.longValue();
                if (val instanceof String s) return Long.parseLong(s);
                return Long.parseLong(String.valueOf(val));

            case "FLOAT":
            case "FLOAT4":
                if (val instanceof Float) return val;
                if (val instanceof Number n) return n.floatValue();
                if (val instanceof String s) return Float.parseFloat(s);
                return Float.parseFloat(String.valueOf(val));

            case "DOUBLE":
            case "FLOAT8":
                if (val instanceof Double) return val;
                if (val instanceof Float f) return (double) f;
                if (val instanceof Number n) return n.doubleValue();
                if (val instanceof String s) return Double.parseDouble(s);
                return Double.parseDouble(String.valueOf(val));

            case "NUMERIC":
            case "DECIMAL":
                if (val instanceof BigDecimal) return val;
                if (val instanceof String s) return new BigDecimal(s);
                if (val instanceof byte[] b) return new BigDecimal(new String(b));
                throw new HaveError("InvalidType", "Invalid decimal value: " + val);

            case "VARCHAR":
            case "CHAR":
            case "BPCHAR":
            case "CHARACTER":
            case "TEXT":
                if (val instanceof String) return val;
                if (val instanceof byte[] b) return new String(b);
                throw new HaveError("InvalidType", "Invalid varchar value: " + val);

            case "TIMESTAMP":
            case "TIMESTAMPTZ":
            case "DATE":
            case "DATETIME":
                if (val instanceof Instant) return val;
                if (val instanceof Long l) return Instant.ofEpochSecond(0, l);
                if (val instanceof String s) return parseDateTime(s);
                throw new HaveError("InvalidType", "Invalid timestamp value: " + val);

            case "BYTEA":
                if (val instanceof byte[]) return val;
                if (val instanceof String s) return Base64.getDecoder().decode(s);
                throw new HaveError("InvalidType", "Invalid bytea value: " + val);

            default:
                // UUID, INTERVAL, BIT, VARBIT, TIMETZ, Array, etc. -> as-is
                return val;
        }
    }

    private static Instant parseDateTime(String s) throws HaveError {
        try {
            OffsetDateTime odt = OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return odt.toInstant();
        } catch (DateTimeParseException e1) {
            try {
                return Instant.parse(s);
            } catch (DateTimeParseException e2) {
                throw new HaveError("InvalidType", "Invalid timestamp string: " + s);
            }
        }
    }
}
