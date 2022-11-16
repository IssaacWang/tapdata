package io.tapdata.connector.tdengine.kit;

import com.taosdata.jdbc.ColumnMetaData;
import com.taosdata.jdbc.TSDBResultSet;
import com.zaxxer.hikari.pool.HikariProxyResultSet;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.parseUBigInt;

/**
 * tools for ResultSet
 *
 * @author Jarad
 * @date 2022/5/29
 */
public class TDengineDbKit {

    /**
     * get all data from ResultSet starting with current row
     *
     * @param resultSet ResultSet
     * @return list<Map>
     */
    public static List<DataMap> getDataFromResultSet(ResultSet resultSet) {
        List<DataMap> list = TapSimplify.list();
        try {
            if (EmptyKit.isNotNull(resultSet)) {
                List<String> columnNames = getColumnsFromResultSet(resultSet);
                //cannot replace with while resultSet.next()
                while (resultSet.next()) {
                    list.add(getRowFromResultSet(resultSet, columnNames));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * get current row
     *
     * @param resultSet   ResultSet
     * @param columnNames column names of ResultSet
     * @return Map
     */
    public static DataMap getRowFromResultSet(ResultSet resultSet, Collection<String> columnNames) {
        DataMap map = DataMap.create();
        try {
            if (EmptyKit.isNotNull(resultSet)) {
                for (String col : columnNames) {
                    map.put(col, getObject(resultSet, col));
                }
                return map;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Object getObject(ResultSet resultSet, String col) throws SQLException, NoSuchFieldException, IllegalAccessException {
        int columnIndex = resultSet.findColumn(col);
        int colType = getColType(resultSet, columnIndex);
        switch (colType) {
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_BIGINT: {
                return resultSet.getInt(columnIndex);
            }
            case TSDB_DATA_TYPE_FLOAT: {
                return resultSet.getFloat(columnIndex);
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                return resultSet.getDouble(columnIndex);
            }
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_JSON: {
                return resultSet.getString(columnIndex);
            }
            default:
                return resultSet.getObject(col);
        }
    }

    public static int getColType(ResultSet resultSet, int columnIndex) throws NoSuchFieldException, IllegalAccessException, SQLException {
        TSDBResultSet tsdbResultSet = resultSet.unwrap(TSDBResultSet.class);
        Field field = tsdbResultSet.getClass().getDeclaredField("columnMetaDataList");
        field.setAccessible(Boolean.TRUE);
        List<ColumnMetaData> columnMetaDataList = (List<ColumnMetaData>) field.get(tsdbResultSet);
        return columnMetaDataList.get(columnIndex - 1).getColType();
    }

    /**
     * get column names from ResultSet
     *
     * @param resultSet ResultSet
     * @return List<String>
     */
    public static List<String> getColumnsFromResultSet(ResultSet resultSet) {
        //get all column names
        List<String> columnNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String[] columnNameArr = resultSetMetaData.getColumnName(i).split("\\.");
                String substring = columnNameArr[columnNameArr.length-1];
                columnNames.add(substring);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public static List<String> getColumnTypesFromResultSet(ResultSet resultSet) {
        //get all column typeNames
        List<String> columnTypeNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnTypeNames.add(resultSetMetaData.getColumnTypeName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnTypeNames;
    }

    public static byte[] blobToBytes(Blob blob) {
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(blob.getBinaryStream());
            byte[] bytes = new byte[(int) blob.length()];
            int len = bytes.length;
            int offset = 0;
            int read;
            while (offset < len && (read = bis.read(bytes, offset, len - offset)) > 0) {
                offset += read;
            }
            return bytes;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String clobToString(Clob clob) {
        String re = "";
        try (Reader is = clob.getCharacterStream(); BufferedReader br = new BufferedReader(is)) {
            String s = br.readLine();
            StringBuilder sb = new StringBuilder();
            while (s != null) {
                sb.append(s);
                s = br.readLine();
            }
            re = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return re;
    }

}
