package io.tapdata.connector.tdengine;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.sql.SQLException;
import java.util.Collections;

public class TDengineRecordWriter extends RecordWriter {

    public TDengineRecordWriter(TDengineJdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}
