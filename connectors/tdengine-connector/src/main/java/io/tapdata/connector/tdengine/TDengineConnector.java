package io.tapdata.connector.tdengine;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.tdengine.bean.TDengineColumn;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.connector.tdengine.ddl.TDengineDDLSqlGenerator;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;

/**
 * @author IssaacWang
 * @date 2022/10/08
 */
@TapConnectorClass("tdengine-spec.json")
public class TDengineConnector extends ConnectorBase {
    private static final String TAG = TDengineConnector.class.getSimpleName();
    private static final int MAX_FILTER_RESULT_SIZE = 100;

    private String version;

    private String connectionTimezone;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;

    private Object slotName; //must be stored in stateMap

    private TDengineConfig tdengineConfig;

    private TDengineJdbcContext tdengineJdbcContext;

    private TDengineDDLSqlGenerator ddlSqlGenerator;

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        tdengineConfig = (TDengineConfig) new TDengineConfig().load(connectionContext.getConnectionConfig());
//        postgresTest = new PostgresTest(postgresConfig);
        if (EmptyKit.isNull(tdengineJdbcContext) || tdengineJdbcContext.isFinish()) {
            tdengineJdbcContext = (TDengineJdbcContext) DataSourcePool.getJdbcContext(tdengineConfig, TDengineJdbcContext.class, connectionContext.getId());
        }
//        this.tDengineJdbcContext = new TDengineJdbcContext(connectionContext);

//        version = tdengineJdbcContext.queryVersion();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        if (EmptyKit.isNotNull(tdengineJdbcContext)) {
            tdengineJdbcContext.finish(connectionContext.getId());
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SSSSSS"));
//        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
//            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
//                tapDateTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
//            }
//            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
//        });
//        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
//            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
//                tapDateValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
//            }
//            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
//        });
//        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);

        connectorFunctions.supportCreateTableV2(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
//        connectorFunctions.supportQueryByAdvanceFilter(this::query);
        connectorFunctions.supportWriteRecord(this::writeRecord);
//        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportReleaseExternalFunction(this::releaseExternal);
    }

    private CreateTableOptions createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (tdengineJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        DataMap nodeConfigMap = tapConnectorContext.getNodeConfig();
        String timestamp = nodeConfigMap.getString("timestamp");
//        String timestamp = "tap_date_time";
//        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE IF NOT EXISTS " + tdengineConfig.getSchema() + "." + tapTable.getId() + "(" + TDengineSqlMaker.buildColumnDefinition(tapTable, timestamp);

        sql += ")";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
//            if (EmptyKit.isNotNull(tapTable.getComment())) {
//                sqls.add("COMMENT ON TABLE \"" + tdengineConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
//            }
//            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
//            for (String fieldName : fieldMap.keySet()) {
//                String fieldComment = fieldMap.get(fieldName).getComment();
//                if (EmptyKit.isNotNull(fieldComment)) {
//                    sqls.add("COMMENT ON COLUMN \"" + tdengineConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
//                }
//            }
            tdengineJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        if (tdengineJdbcContext.tableExists(tableId)) {
            String sql = String.format("DELETE FROM %s.%s", tdengineConfig.getSchema(), tableId);
            tdengineJdbcContext.execute(sql);
        } else {
            TapLogger.warn(TAG, "Table \"{}.{}\" not exists, will skip clear table", tdengineConfig.getSchema(), tableId);
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        String tableId = tapDropTableEvent.getTableId();
        String sql = String.format("DROP TABLE IF EXISTS %s.%s", tdengineConfig.getSchema(), tableId);
        tdengineJdbcContext.execute(sql);
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new TDengineRecordWriter(tdengineJdbcContext, tapTable)
//                .setVersion(postgresVersion)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .write(tapRecordEvents, consumer);
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {

    }

    private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) throws Throwable {

    }

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
//        mysqlReader.readBinlog(tapConnectorContext, tables, offset, batchSize, DDL_PARSER_TYPE, consumer);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        int count = 0;
        try {
//            count = tdengineJdbcContext.count(tapTable.getName());
        } catch (Exception e) {
            throw new RuntimeException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
        }
        return count;
    }

    private TapRecordEvent tapRecordWrapper(TapConnectorContext tapConnectorContext, Map<String, Object> before, Map<String, Object> after, TapTable tapTable, String op) {
        TapRecordEvent tapRecordEvent;
        switch (op) {
            case "i":
                tapRecordEvent = TapSimplify.insertRecordEvent(after, tapTable.getId());
                break;
            case "u":
                tapRecordEvent = TapSimplify.updateDMLEvent(before, after, tapTable.getId());
                break;
            case "d":
                tapRecordEvent = TapSimplify.deleteDMLEvent(before, tapTable.getId());
                break;
            default:
                throw new IllegalArgumentException("Operation " + op + " not support");
        }
        tapRecordEvent.setConnector(tapConnectorContext.getSpecification().getId());
        tapRecordEvent.setConnectorVersion(version);
        return tapRecordEvent;
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        return null;
//        if (null == startTime) {
//            return this.mysqlJdbcContext.readBinlogPosition();
//        } else {
//            throw new NotSupportedException();
//        }
    }

    private void createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapCreateIndexEvent tapCreateIndexEvent) throws Throwable {

    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {

    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                tdengineJdbcContext.execute(sql);
            } catch (Throwable e) {
                throw new RuntimeException("Execute ddl sql failed: " + sql + ", error: " + e.getMessage(), e);
            }
        }
    }

    private void releaseExternal(TapConnectorContext tapConnectorContext) {
        try {
            KVMap<Object> stateMap = tapConnectorContext.getStateMap();
            if (null != stateMap) {
                stateMap.clear();
            }
        } catch (Throwable throwable) {
            TapLogger.warn(TAG, "Release tdengine state map failed, error: " + throwable.getMessage());
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        //get table info
        List<DataMap> tableList = tdengineJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("table_name")).collect(Collectors.toList());
            System.out.println(subTableNames.get(0));
            List<DataMap> columnList = tdengineJdbcContext.queryAllColumns(subTableNames);
//            List<DataMap> indexList = tdengineJdbcContext.queryAllIndexes(subTableNames);
            //make up tapTable
            subList.forEach(subTable -> {
                //1、table name/comment
                String table = subTable.getString("table_name");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("comment"));
                //2、primary key and table index (find primary key from index info)
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
//                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("table_name")))
//                        .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
//                indexMap.forEach((key, value) -> {
//                    if (value.stream().anyMatch(v -> (boolean) v.get("is_primary"))) {
//                        primaryKey.addAll(value.stream().map(v -> v.getString("column_name")).collect(Collectors.toList()));
//                    }
//                    TapIndex index = index(key);
//                    value.forEach(v -> index.indexField(indexField(v.getString("column_name")).fieldAsc("A".equals(v.getString("asc_or_desc")))));
//                    index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
//                    index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("is_primary")));
//                    tapIndexList.add(index);
//                });
                //3、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("table_name")))
                        .forEach(col -> {
                            TapField tapField = new TDengineColumn(col).getTapField(); //make up fields
                            tapField.setPos(keyPos.incrementAndGet());
//                            tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
//                            tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                            tapTable.add(tapField);
                        });
                tapTable.setIndexList(tapIndexList);
                tapTableList.add(tapTable);
            });
            consumer.accept(tapTableList);
        });
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        onStart(connectionContext);
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        TDengineConnectionTest tdengineConnectionTest = new TDengineConnectionTest(tdengineJdbcContext);
        TestItem testHostPort = tdengineConnectionTest.testHostPort(connectionContext);
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem testConnect = tdengineConnectionTest.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem testDatabaseVersion = tdengineConnectionTest.testDatabaseVersion();
        consumer.accept(testDatabaseVersion);
        if (testDatabaseVersion.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem binlogMode = tdengineConnectionTest.testBinlogMode();
        TestItem binlogRowImage = tdengineConnectionTest.testBinlogRowImage();
        TestItem cdcPrivileges = tdengineConnectionTest.testCDCPrivileges();
        consumer.accept(binlogMode);
        consumer.accept(binlogRowImage);
        consumer.accept(cdcPrivileges);
        consumer.accept(tdengineConnectionTest.testCreateTablePrivilege(connectionContext));
//        if (binlogMode.isSuccess() && binlogRowImage.isSuccess() && cdcPrivileges.isSuccess()) {
//            List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDL_PARSER_TYPE);
//            ddlCapabilities.forEach(connectionOptions::capability);
//        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(tdengineConfig, tapNewFieldEvent);
    }

    private List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(tdengineConfig, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(tdengineConfig, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(tdengineConfig, tapAlterFieldNameEvent);
    }

}
