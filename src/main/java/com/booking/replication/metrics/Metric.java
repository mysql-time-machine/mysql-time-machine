package com.booking.replication.metrics;

/**
 * Created by bosko on 12/24/15.
 */
public class Metric {

    // Event counters
    public static final int INSERT_EVENTS_COUNTER = 0;
    public static final int UPDATE_EVENTS_COUNTER = 1;
    public static final int DELETE_EVENTS_COUNTER = 2;
    public static final int COMMIT_COUNTER        = 3;
    public static final int XID_COUNTER           = 4;

    public static final int EVENTS_PROCESSED = 5;
    public static final int EVENTS_SKIPPED   = 6;
    public static final int EVENTS_RECEIVED  = 7;

    // Row counters
    public static final int ROWS_PROCESSED = 101;
    public static final int ROWS_INSERTED  = 102;
    public static final int ROWS_UPDATED   = 103;
    public static final int ROWS_DELETED   = 104;
    public static final int ROWS_APPLIED   = 105;

    // General counters
    public static final int HEART_BEAT_COUNTER      = 1001;
    public static final int APPLIER_TASKS_SUCCEEDED = 1002;

    // == Gauges ==

    // Replication delay
    public static final int REPLICATION_DELAY_MS = 2001;

    public static String getCounterName(int metricID) {
        if (metricID == INSERT_EVENTS_COUNTER) {
            return "INSERT_EVENTS_COUNT";
        }
        else if (metricID == UPDATE_EVENTS_COUNTER) {
            return  "UPDATE_EVENTS_COUNT";
        }
        else if (metricID == DELETE_EVENTS_COUNTER) {
            return "DELETE_EVENTS_COUNT";
        }
        else if (metricID == COMMIT_COUNTER) {
            return "COMMIT_QUERIES_COUNT";
        }
        else if (metricID == XID_COUNTER ) {
            return "XID_COUNT";
        }
        else if (metricID == EVENTS_RECEIVED) {
            return "EVENTS_RECEIVED";
        }
        else if (metricID == EVENTS_PROCESSED) {
            return "EVENTS_PROCESSED";
        }
        else if (metricID == EVENTS_SKIPPED) {
            return "EVENTS_SKIPPED";
        }
        else if (metricID == ROWS_PROCESSED) {
            return "ROWS_PROCESSED";
        }
        else if (metricID == ROWS_INSERTED) {
            return "ROWS_INSERTED";
        }
        else if (metricID == ROWS_UPDATED) {
            return "ROWS_UPDATED";
        }
        else if (metricID == ROWS_DELETED) {
            return "ROWS_DELETED";
        }
        else if (metricID == ROWS_APPLIED) {
            return "ROWS_APPLIED";
        }
        else if (metricID == HEART_BEAT_COUNTER ) {
            return "HEART_BEAT_COUNTER";
        }
        else if (metricID == APPLIER_TASKS_SUCCEEDED ) {
            return "APPLIER_TASKS_SUCCEEDED";
        }
        else if (metricID == REPLICATION_DELAY_MS) {
            return "REPLICATION_DELAY_MS";
        }
        else {
            return "NA";
        }
    }
}
