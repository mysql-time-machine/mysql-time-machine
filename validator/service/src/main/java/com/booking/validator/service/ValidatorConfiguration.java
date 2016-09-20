package com.booking.validator.service;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by psalimov on 9/16/16.
 */
public class ValidatorConfiguration {

    public static class DataSource {

        private String name;
        private String type;

        private Map<String,String> configuration;

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public Map<String, String> getConfiguration() {
            return configuration;
        }

    }

    public static class TaskSupplier {

        private String type;
        private Map<String,String> configuration;

        public String getType() {
            return type;
        }

        public Map<String, String> getConfiguration() {
            return configuration;
        }
    }

    @JsonProperty("data_sources")
    private Iterable<DataSource> dataSources;

    @JsonProperty("task_supplier")
    private TaskSupplier taskSupplier;

    public Iterable<DataSource> getDataSources() {
        return dataSources;
    }

    public TaskSupplier getTaskSupplier() {
        return taskSupplier;
    }
}
