package com.booking.validation;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by lezhong on 7/14/16.
 */

public class Comparator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Comparator.class);
    private static final String rowsPassTotal = "COLUMNS_PASS_TOTAL";
    private static final String rowsFailTotal = "COLUMNS_FAIL_TOTAL";
    private static final String columnsPassTotal = "IDS_PASS_TOTAL";
    private static final String columnsFailTotal = "IDS_FAIL_TOTAL";
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final int partitionLength = 1;
    private static final String consUPDATE = "UPDATE";
    private static final String consINSERT = "INSERT";
    private static final String consDELETE = "DELETE";
    private ConfigurationKafka configurationKafka;
    private ConfigurationHBase configurationHBase;

    Comparator(ConfigurationKafka configKafka, ConfigurationHBase configHBase) {
        configurationKafka = configKafka;
        configurationHBase = configHBase;
    }

    public void compareMySQLandHBase() {
        String username = configurationHBase.getMySQLUsername();
        String password = configurationHBase.getPassword();
        String dbName = configurationHBase.getdbName();
        // Config dbConfig = configurationHBase.get_config(dbName);
        String dataSource = String.format("dbi:mysql:%s;host=%s", dbName, configurationHBase.getMySQLServer());
        String dataSourceInfo = String.format("dbi:mysql:information_schema;host=%s", configurationHBase.getMySQLServer());

        MySQLConnector dbhInfo = new MySQLConnector(username, password, configurationHBase.getMySQLServer());
        ArrayList<MySQLConnector.ColumnTypes> columnTypes = dbhInfo.getColumnTypes(dbName, configurationHBase.getTable());

        // Value Match TODO: validating

        // Hbase Connection

        ArrayList<String> ids = dbhInfo.getIds(dbName, configurationHBase.getTable());

        System.out.println(String.format("Total of %d ids from table %s will be tested, split info %d chunks.",
                ids.size(), configurationHBase.getTable(), ids.size() / partitionLength));

        HashMap<String, Integer> stats = new HashMap<>();
        stats.put(rowsPassTotal, 0);
        stats.put(rowsFailTotal, 0);
        stats.put(columnsPassTotal, 0);
        stats.put(columnsFailTotal, 0);

        // get_tests();
        for (int chunkNo = 0;chunkNo < ids.size(); chunkNo += partitionLength) {
            List<String> chunk = ids.subList(chunkNo, chunkNo + partitionLength);
            // System.out.println(String.format("Comparator chunk %d...", chunkNo));
            if (chunkNo == 0) {
                System.out.println(new String(new char[80]).replace('\0', '-'));
                System.out.println(String.format("%sPASS: { rows => %9d, columns => %9d }%s", ANSI_GREEN,
                        stats.get(columnsPassTotal), stats.get(""), ANSI_RESET));
                System.out.println(ANSI_RED + "FAIL: {}" + ANSI_RESET);
            }
            HashMap<String, List<String>> chunkHash = new HashMap<>();
            chunkHash.put("id", chunk);
            HashMap<String, HashMap<String, String>> mySQLRows = dbhInfo.getMySQLRows(dbName, configurationHBase.getTable(), chunkHash);
            // HBaseRows = getHBaseRows(chunk);
            for (String id: chunk) {
                HashMap<String, String> myRow = mySQLRows.get(id);
                HashMap<String, String> hbRow = new HashMap<String, String>();
            }
        }
    }

    public void compareMySQLandKafka() {
        Validating validator = new Validating();
        String username = configurationKafka.getMySQLUsername();
        String password = configurationKafka.getPassword();
        String dbName = configurationKafka.getdbName();
        MySQLConnector dbhInfo = new MySQLConnector(username, password, configurationKafka.getMySQLHost());
        HashMap<String, Integer> stats = new HashMap<>();
        stats.put(rowsPassTotal, 0);
        stats.put(rowsFailTotal, 0);
        stats.put(columnsPassTotal, 0);
        stats.put(columnsFailTotal, 0);

        KafkaConnector kafkaConnector = new KafkaConnector(configurationKafka);
        for (int count = 0; count < configurationKafka.getTestingRound(); count ++ ) {
            JSONObject val = kafkaConnector.nextKeyValue();
            String type = val.get("eventType").toString();
            String tableName = val.get("tableName").toString();
            JSONArray pkSet = (JSONArray) val.get("primaryKeyColumns");
            JSONObject eventColumns = (JSONObject) val.get("eventColumns");
            HashMap<String, List<String>> pks = new HashMap<>();
            for (int ind = 0;ind < pkSet.size(); ind ++) {
                String key = pkSet.get(ind).toString();
                List<String> idValue = new ArrayList<>();
                JSONObject valueTuples = (JSONObject) eventColumns.get(key);
                switch (type) {
                    case consUPDATE: {
                        idValue.add(valueTuples.get("value_after").toString());
                    } break;
                    case consINSERT: case consDELETE: {
                        idValue.add(valueTuples.get("value").toString());
                    } break;
                    default: break;
                }
                pks.put(key, idValue);
            }
            HashMap<String, HashMap<String, String>> mySQLRows = dbhInfo.getMySQLRows(dbName, tableName, pks);
            if (type.equals(consDELETE)) {
                if (mySQLRows != null) {
                    stats.put(rowsFailTotal, stats.get(rowsFailTotal) + 1);
                } else {
                    stats.put(rowsPassTotal, stats.get(rowsPassTotal) + 1);
                }
            } else {
                for (String key : mySQLRows.keySet()) {
                    HashMap<String, String> mySQLRow = mySQLRows.get(key);
                    Boolean fail = false;
                    switch (type) {
                        case consUPDATE: {
                            for (Object columnKey : eventColumns.keySet()) {
                                JSONObject kafkaValue = (JSONObject) eventColumns.get(columnKey);
                                String valueType = kafkaValue.get("type").toString();
                                String valueFromMySQL = mySQLRow.get(columnKey.toString());
                                String valueFromKafka = kafkaValue.get("value_after").toString();
                                Boolean res = validator.comparisonHelper(valueType, valueFromMySQL, valueFromKafka);
                                if (!res) {
                                    fail = true;
                                    stats.put(columnsFailTotal, stats.get(columnsFailTotal) + 1);
                                    System.out.println(String.format("type: %s, id: %s, column: %s, value: %s != %s",
                                            valueType, key, columnKey, valueFromMySQL, valueFromKafka));
                                } else {
                                    stats.put(columnsPassTotal, stats.get(columnsPassTotal) + 1);
                                }
                            }
                            break;
                        }
                        case consINSERT: {
                            for (Object columnKey : eventColumns.keySet()) {
                                JSONObject kafkaValue = (JSONObject) eventColumns.get(columnKey);
                                String valueType = kafkaValue.get("type").toString();
                                String valueFromMySQL = mySQLRow.get(columnKey.toString());
                                String valueFromKafka = kafkaValue.get("value").toString();
                                Boolean res = validator.comparisonHelper(valueType, valueFromMySQL, valueFromKafka);
                                if (!res) {
                                    fail = true;
                                    stats.put(columnsFailTotal, stats.get(columnsFailTotal) + 1);
                                    System.out.println(String.format("type %s, id: %s, column: %s, value: %s != %s",
                                            valueType, key, columnKey, valueFromMySQL, valueFromKafka));
                                } else {
                                    stats.put(columnsPassTotal, stats.get(columnsPassTotal) + 1);
                                }
                            }
                            break;
                        }
                        default:
                            break;
                    }
                    if (fail) {
                        LOGGER.info(eventColumns.toJSONString());
                        stats.put(rowsFailTotal, stats.get(rowsFailTotal) + 1);
                    } else {
                        stats.put(rowsPassTotal, stats.get(rowsPassTotal) + 1);
                    }
                }
            }
        }
        System.out.println(new String(new char[80]).replace('\0', '-'));
        System.out.println(String.format("%sPASS: { rows => %9d, columns => %9d }%s", ANSI_GREEN,
                stats.get(rowsPassTotal), stats.get(columnsPassTotal), ANSI_RESET));
        System.out.println(String.format("%sFAIL: { rows => %9d, columns => %9d }%s", ANSI_RED,
                stats.get(rowsFailTotal), stats.get(columnsFailTotal), ANSI_RESET));
    }
}
