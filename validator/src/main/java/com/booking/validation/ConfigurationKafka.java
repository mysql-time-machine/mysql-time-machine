package com.booking.validation;

import com.booking.validation.util.Duration;
import com.booking.validation.util.StartupParameters;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;
import org.apache.htrace.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.HashMap;
import java.util.List;

/**
 * Created by lezhong on 8/12/16.
 */

public class ConfigurationKafka {
    ConfigurationKafka() {}

    @JsonDeserialize
    @JsonProperty("replication_schema")
    public ReplicationSchema replication_schema = new ReplicationSchema();

    private static class ReplicationSchema {
        public String       name;
        public String       username;
        public String       password;
        public String       host;
    }

    @JsonDeserialize
    public KafkaConfiguration kafka = new KafkaConfiguration();

    private static class KafkaConfiguration {
        public String broker;
        public String topic;
        public List<String> tables;
        public List<String> excludetables;
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

    public int getTestingRound() {
        return 100;
    }

    public String getKafkaBroker() {
        return kafka.broker;
    }

    public String getKafkaTopicName() {
        return kafka.topic;
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

    public String getMySQLHost() {
        return replication_schema.host;
    }

    public void loadStartupParameters(StartupParameters startupParameters ) {
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
        if (kafka.broker == null) {
            throw new RuntimeException("Kafka broker address cannot be null.");
        }
        if (kafka.topic == null) {
            throw new RuntimeException("Kafka topic cannot be null.");
        }
    }

}
