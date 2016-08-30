package com.booking.validation;

import com.google.common.base.Joiner;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by lezhong on 8/10/16.
 */

public class MySQLConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnector.class);
    static Connection conn;
    static Statement stmt;
    static String id;
    ConfigurationHBase configurationHBase = new ConfigurationHBase();

    MySQLConnector(String user, String pass, String dbHost) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            LOGGER.info("Connecting to database...");
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUser(user);
            dataSource.setPassword(pass);
            dataSource.setServerName(dbHost);
            dataSource.setZeroDateTimeBehavior("convertToNull");
            conn = dataSource.getConnection();
            LOGGER.info("Creating statement...");
        } catch (Exception se) {
            se.printStackTrace();
        }
    }

    static ResultSet executeSQL(String sql) {
        try {
            stmt = conn.createStatement();
            return stmt.executeQuery(sql);
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    static class ColumnTypes {
        String dataType;
        String columnType;
        String charSet;
        String colation;
    }

    public ArrayList<ColumnTypes> getColumnTypes(String db, String table) {
        String sql = String.format("SELECT "
                + "COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME "
                + "FROM "
                + "INFORMATION_SCHEMA.COLUMNS "
                + "WHERE "
                + "TABLE_SCHEMA = '%s' "
                + "AND "
                + "TABLE_NAME = '%s'", db, table);

        System.out.println(sql);

        ResultSet rst = executeSQL(sql);

        ArrayList<ColumnTypes> result = new ArrayList<>();
        try {
            while (rst.next()) {
                ColumnTypes col = new ColumnTypes();
                // TODO: add ORDINAL_POSITION and COLUMN_KEY
                col.dataType = rst.getString("DATA_TYPE");
                col.columnType = rst.getString("COLUMN_TYPE");
                col.charSet = rst.getString("CHARACTER_SET_NAME");
                col.colation = rst.getString("COLLATION_NAME");
                result.add(col);
            }
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return result;
    }

    private void selectDB(String db) {
        String sql;
        sql = String.format("use %s", db);
        executeSQL(sql);
    }

    public ArrayList<String> getIds(String db, String table) {
        id = configurationHBase.getTableID(db, table);
        String sql;
        selectDB(db);
        sql = configurationHBase.getTableSQL(id, table);
        System.out.println(sql);
        ResultSet rst = executeSQL(sql);
        ArrayList<String> result = new ArrayList<>();
        try {
            while (rst.next()) {
                result.add(configurationHBase.processID(id, rst));
            }
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return result;
    }

    public HashMap<String, HashMap<String, String>> getMySQLRows(String db, String table, HashMap<String, List<String>> ids) {
        String sql;

        selectDB(db);
        String whereClause = "";
        for (String key: ids.keySet()) {
            if (whereClause.length() > 0) {
                whereClause = whereClause.concat(" AND ");
            }
            String idsString = Joiner.on(",").join(ids.get(key));
            whereClause = whereClause.concat(String.format("%s in (%s)", key, idsString));
        }
        sql = String.format("SELECT * FROM %s WHERE %s", table, whereClause);
        System.out.println(sql);
        try {
            ResultSet rs = executeSQL(sql);
            java.sql.ResultSetMetaData rsmd = rs.getMetaData();
            List<String> columns = new ArrayList<>(rsmd.getColumnCount());
            for (int i = 1;i <= rsmd.getColumnCount();i ++) {
                columns.add(rsmd.getColumnName(i));
            }
            HashMap<String, HashMap<String, String>> data = new HashMap<>();
            while (rs.next()) {
                HashMap<String, String> row = new HashMap<>(columns.size());
                for (String col: columns) {
                    row.put(col, rs.getString(col));
                }
                String key = "";
                for (String id: ids.keySet()) {
                    if (key.length() == 0) {
                        key = rs.getString(id);
                    } else {
                        key = key.concat(";" + rs.getString(id));
                    }
                }
                data.put(key, row);
            }
            return data;
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }
}
