package io.tapdata.connector.tdengine.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;

/**
 * @author IssaacWang
 * @date 2022/10/08
 */
public class TDengineColumn extends CommonColumn {

    public TDengineColumn() {

    }

    public TDengineColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("field");
        this.dataType = dataMap.getString("type");
//        this.dataType = dataMap.getString("data_type");
//        this.nullable = dataMap.getString("is_nullable");
        this.remarks = dataMap.getString("note");
        //create table in target has no need to set default value
        this.columnDefaultValue = null;
//        this.columnDefaultValue = getDefaultValue(dataMap.getString("column_default"));
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "YES".equals(this.nullable);
    }

    private String getDefaultValue(String defaultValue) {
        if (EmptyKit.isNull(defaultValue) || defaultValue.startsWith("NULL::")) {
            return null;
        } else if (defaultValue.contains("::")) {
            return defaultValue.substring(0, defaultValue.lastIndexOf("::"));
        } else {
            return defaultValue;
        }
    }
}
