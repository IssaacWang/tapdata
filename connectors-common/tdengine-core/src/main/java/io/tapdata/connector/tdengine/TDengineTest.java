package io.tapdata.connector.tdengine;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.util.UUID;

import static io.tapdata.base.ConnectorBase.testItem;

// TODO: 2022/6/9 need to improve test items 
public class TDengineTest extends CommonDbTest {
    private TDengineJdbcContext tdengineJdbcContext;

    public TDengineTest() {
        super();
    }

    public TDengineTest(TDengineConfig tdengineConfig) {
        super(tdengineConfig);
        jdbcContext = (TDengineJdbcContext) DataSourcePool.getJdbcContext(tdengineConfig, TDengineJdbcContext.class, uuid);
    }

//    public TestItem testHostPort(TapConnectionContext tapConnectionContext) {
//        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
//        String host = String.valueOf(connectionConfig.get("host"));
//        int port = ((Number) connectionConfig.get("port")).intValue();
//        try {
//            NetUtil.validateHostPortWithSocket(host, port);
//            return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
//        } catch (IOException e) {
//            return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
//        }
//    }
//
//    public TestItem testConnect() {
//        try (
//                Connection connection = tdengineJdbcContext.getConnection()
//        ) {
//            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
//        } catch (Exception e) {
//            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
//        }
//    }

    @Override
    public void close() {
        try {
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

}
