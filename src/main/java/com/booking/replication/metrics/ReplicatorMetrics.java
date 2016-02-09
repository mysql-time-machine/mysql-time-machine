package com.booking.replication.metrics;

import com.booking.replication.util.MutableLong;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bosko on 1/19/16.
 */
public class ReplicatorMetrics {
    private final ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> replicatorMetrics;

    public ReplicatorMetrics() {
        replicatorMetrics = new  ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>>();
    }

    public ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> getMetrics() {
        return replicatorMetrics;
    }

    public void initTimebucketIfKeyDoesNotExists(int currentTimeSeconds) {

        if (this.replicatorMetrics.containsKey(currentTimeSeconds)) {
            return;
        }
        else {
            this.replicatorMetrics.put(currentTimeSeconds, new HashMap<Integer, MutableLong>());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.EVENTS_RECEIVED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.EVENTS_SKIPPED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.EVENTS_PROCESSED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.INSERT_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.UPDATE_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.DELETE_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.COMMIT_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.XID_COUNTER, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_PROCESSED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_INSERTED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_UPDATED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_DELETED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.ROWS_APPLIED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.HEART_BEAT_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.APPLIER_TASKS_SUCCEEDED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Metric.REPLICATION_DELAY_MS, new MutableLong());
        }
    }

    // ROWS
    public void incRowsInsertedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROWS_INSERTED).increment();
    }

    public void incRowsUpdatedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROWS_UPDATED).increment();
    }

    public void incRowsDeletedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROWS_DELETED).increment();
    }

    public void incRowsProcessedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROWS_PROCESSED).increment();
    }
    public void incRowsAppliedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.ROWS_APPLIED).increment();
    }

    // EVENTS
    public void incInsertEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.INSERT_EVENTS_COUNTER).increment();
    }

    public void incUpdateEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.UPDATE_EVENTS_COUNTER).increment();
    }

    public void incDeleteEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.DELETE_EVENTS_COUNTER).increment();
    }

    public void incCommitQueryCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.COMMIT_COUNTER).increment();
    }

    public void incXIDCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.XID_COUNTER);
    }

    public void incEventsReceivedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.EVENTS_RECEIVED).increment();
    }

    public void incEventsProcessedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.EVENTS_PROCESSED).increment();
    }

    public void incEventsSkippedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.EVENTS_SKIPPED).increment();
    }

    public void incHeartBeatCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.HEART_BEAT_COUNTER).increment();
    }

    public void incApplierTasksSucceededCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.APPLIER_TASKS_SUCCEEDED).increment();
    }

    // TODO: add separate metrics for commitedToHBaseReplicationDelay
    public void setReplicatorReplicationDelay(Long replicatorReplicationDelay) {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Metric.REPLICATION_DELAY_MS).setValue(replicatorReplicationDelay);
    }
}
