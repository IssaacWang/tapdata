package io.tapdata.connector.tdengine.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class TDengineConfig extends CommonDbConfig implements Serializable {

    //customize
    public TDengineConfig() {
        setDbType("TAOS");
        setJdbcDriver("com.taosdata.jdbc.TSDBDriver");
    }

}
