package com.booking.validation;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Created by lezhong on 7/14/16.
 */

public class Sampling {
    ConfigurationHBase configurationHBase = new ConfigurationHBase();

    public class MySQLGenerator {
        //  Database credentials
        final String user = configurationHBase.getMySQLUsername();
        final String pass = configurationHBase.getPassword();
        private String sql;
        private ResultSet rs;
        private ArrayList<String> tableList = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        MysqlDataSource dataSource;

        MySQLGenerator() {
            try {
                // STEP 2: Register JDBC driver
                Class.forName("com.mysql.jdbc.Driver");

                // STEP 3: Open a connection
                dataSource = new MysqlDataSource();
                dataSource.setUser(user);
                dataSource.setPassword(pass);
                dataSource.setServerName(configurationHBase.getMySQLServer());
                conn = dataSource.getConnection();

                // STEP 4: Execute a query
                stmt = conn.createStatement();
                sql = String.format("use %s;", configurationHBase.getdbName());
                stmt.executeQuery(sql);
                stmt.close();
            } catch (SQLException se) {
                // Handle errors for JDBC
                se.printStackTrace();
            } catch (Exception e) {
                // Handle errors for Class.forName
                e.printStackTrace();
            }
        }

        ArrayList<String> getTableList() throws Exception {
            stmt = conn.createStatement();
            sql = "show tables";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                tableList.add(rs.getNString(1));
            }
            rs.close();
            stmt.close();
            return tableList;
        }

        ResultSet getResult(String table, int offset) throws SQLException {
            sql = String.format("SELECT * FROM %s LIMIT 1 OFFSET %d;", table, offset);
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            return rs;
        }

        void next() {

        }
    }


    public class HBaseGenerator {
        private Configuration config;
        private HTable table;

        HBaseGenerator() {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", configurationHBase.getHBaseZKQuorum());
        }

        void setTable(String tableName) throws IOException {
            table = new HTable(config, tableName);
        }

        Result getResult(String rowName) throws IOException {
            Get hbaseGet = new Get(Bytes.toBytes(rowName));
            Result result = table.get(hbaseGet);
            return result;
        }

        void next() {

        }
    }

}
