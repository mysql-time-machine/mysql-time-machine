package com.booking.validation;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static class MySQLGenerator {
        //  Database credentials
        static final String USER = com.booking.validation.Configuration.getMySQLUsername();
        static final String PASS = com.booking.validation.Configuration.getPassword();
        private static String sql;
        private static ResultSet rs;
        private static ArrayList<String> tableList = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        MysqlDataSource dataSource;
        private static final Logger LOGGER = LoggerFactory.getLogger(MySQLGenerator.class);

        MySQLGenerator() {
            try {
                // STEP 2: Register JDBC driver
                Class.forName("com.mysql.jdbc.Driver");

                // STEP 3: Open a connection
                LOGGER.info("Connecting to database...");
                dataSource = new MysqlDataSource();
                dataSource.setUser(USER);
                dataSource.setPassword(PASS);
                dataSource.setServerName(com.booking.validation.Configuration.getMySQLServer());
                conn = dataSource.getConnection();

                // STEP 4: Execute a query
                LOGGER.info("Creating statement...");
                stmt = conn.createStatement();
                sql = String.format("use %s;", com.booking.validation.Configuration.getdbName());
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


    public static class HBaseGenerator {
        private Configuration config;
        private HTable table;
        private static final Logger LOGGER = LoggerFactory.getLogger(MySQLGenerator.class);

        HBaseGenerator() {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", com.booking.validation.Configuration.getHBaseZKQuorum());
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
