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

    private String logPluginName = "tdengineoutput"; //default log plugin for tdengine, pay attention to lower version

    //customize
    public TDengineConfig() {
        setDbType("tdengine");
        setJdbcDriver("com.taosdata.jdbc.TSDBDriver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }
}
