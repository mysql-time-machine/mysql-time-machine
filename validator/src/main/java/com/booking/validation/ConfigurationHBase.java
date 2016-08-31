package com.booking.validation;

import static com.booking.validation.HBaseIDFetcher.getTablePrimaryID;

import com.booking.validation.util.Duration;
import com.booking.validation.util.StartupParameters;
import com.google.common.base.Joiner;

import org.apache.commons.lang.StringUtils;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;
import org.apache.htrace.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by lezhong on 8/12/16.
 */

public class ConfigurationHBase {
    ConfigurationHBase() {}

    @JsonDeserialize
    public ReplicationSchema replication_schema = new ReplicationSchema();

    private static class ReplicationSchema {
        public String       name;
        public String       username;
        public String       password;
        public String       host;
    }

    @JsonDeserialize
    public HBaseConfiguration hbase = new HBaseConfiguration();

    private static class HBaseConfiguration {
        public String namespace;
        public List<String> zookeeper_quorum;
    }

    public String getHbaseNamespace() {
        if (hbase != null) {
            return hbase.namespace;
        } else {
            return null;
        }
    }

    public String getHbaseQuorum() {
        if (hbase != null) {
            return Joiner.on(",").join(hbase.zookeeper_quorum);
        } else {
            return null;
        }
    }

    @JsonDeserialize
    @JsonProperty("metrics")
    public MetricsConfig metrics = new MetricsConfig();

    public static class MetricsConfig {
        public Duration frequency;
        @JsonDeserialize
        public HashMap<String, ReporterConfig> reporters = new HashMap<>();

        public static class ReporterConfig {
            public String type;
            public String namespace;
            public String url;
        }
    }

    public static int getTestingRound() {
        return 100;
    }

    public String getMySQLUsername() {
        return replication_schema.username;
    }

    public String getPassword() {
        return replication_schema.password;
    }

    public String getdbName() {
        return replication_schema.name;
    }

    public String getHBaseZKQuorum() {
        return StringUtils.join(hbase.zookeeper_quorum, ",");
    }

    public String getMySQLServer() {
        return replication_schema.host;
    }

    public String getTableID(String db, String table) {
        return getTablePrimaryID(String.format("schema_history:%s", db), table);
    }

    public String getTableSQL(String id, String table) {
        return String.format("SELECT %s FROM %s LIMIT 1000", id, table);
    }

    public String processID(String id, ResultSet rst) {
        String[] list = id.split(",");
        try {
            String resId = rst.getString(list[0]);
            for (int i = 1;i < list.length;i ++) {
                resId = String.format("%s;%s", resId, rst.getString(list[i]));
            }
            return resId;
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return "";
    }

    private StartupParameters startupParameters;

    public void loadStartupParameters(StartupParameters startParam) {
        startupParameters = startParam;
    }

    public String getTable() {
        return startupParameters.getTable();
    }

    /**
     * Validate configuration.
     */
    public void validate() {

        if (replication_schema.name == null) {
            throw new RuntimeException("Replication schema name cannot be null.");
        }
        if (replication_schema.host == null) {
            throw new RuntimeException("Replication schema host name cannot be null.");
        }
        if (replication_schema.username == null) {
            throw new RuntimeException("Replication schema user name cannot be null.");
        }
        if (hbase.namespace == null) {
            throw new RuntimeException("HBase namespace cannot be null.");
        }
        if (hbase.zookeeper_quorum == null) {
            throw new RuntimeException("HBase zookeeper quorum cannot be null.");
        }
    }

}
