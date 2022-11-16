package io.tapdata.connector.tdengine;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TDengineWriteRecorder extends WriteRecorder {

    public TDengineWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        justInsert(after);
        preparedStatement.addBatch();
    }

    //just insert
    private void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    @Override
    public void addUpdateBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after) || EmptyKit.isEmpty(uniqueCondition)) {
            return;
        }
        Map<String, Object> before = new HashMap<>();
        uniqueCondition.forEach(k -> before.put(k, after.get(k)));
        if (updatePolicy.equals(ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS)) {
            insertUpdate(after, before);
        } else {
            justUpdate(after, before);
        }
        preparedStatement.addBatch();
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
    }

    private void insertUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String updateSql;
            if (hasPk) {
                updateSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + before.keySet().stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            } else {
                updateSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            }
            preparedStatement = connection.prepareStatement(updateSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    @Override
    public void addDeleteBatch(Map<String, Object> before) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            before.keySet().removeIf(k -> !uniqueCondition.contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + schema + "\".\"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + schema + "\".\"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }
}
