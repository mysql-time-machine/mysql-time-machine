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

            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.EVENTS_RECEIVED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.EVENTS_SKIPPED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.EVENTS_PROCESSED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.INSERT_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.UPDATE_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.DELETE_EVENTS_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.COMMIT_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.XID_COUNTER, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.ROWS_PROCESSED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.ROWS_INSERTED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.ROWS_UPDATED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.ROWS_DELETED, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.ROWS_APPLIED, new MutableLong());

            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.HEART_BEAT_COUNTER, new MutableLong());
            this.replicatorMetrics.get(currentTimeSeconds).put(Counters.APPLIER_TASKS_SUCCEEDED, new MutableLong());
        }
    }

    // ROWS
    public void incRowsInsertedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.ROWS_INSERTED).increment();
    }

    public void incRowsUpdatedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.ROWS_UPDATED).increment();
    }

    public void incRowsDeletedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.ROWS_DELETED).increment();
    }

    public void incRowsProcessedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.ROWS_PROCESSED).increment();
    }
    public void incRowsAppliedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.ROWS_APPLIED).increment();
    }

    // EVENTS
    public void incInsertEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.INSERT_EVENTS_COUNTER).increment();
    }

    public void incUpdateEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.UPDATE_EVENTS_COUNTER).increment();
    }

    public void incDeleteEventCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.DELETE_EVENTS_COUNTER).increment();
    }

    public void incCommitQueryCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.COMMIT_COUNTER).increment();
    }

    public void incXIDCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.XID_COUNTER);
    }

    public void incEventsReceivedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.EVENTS_RECEIVED).increment();
    }

    public void incEventsProcessedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.EVENTS_PROCESSED).increment();
    }

    public void incEventsSkippedCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.EVENTS_SKIPPED).increment();
    }

    public void incHeartBeatCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.HEART_BEAT_COUNTER).increment();
    }

    public void incApplierTasksSucceededCounter() {
        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);
        if (this.replicatorMetrics.get(currentTimeSeconds) == null) {
            initTimebucketIfKeyDoesNotExists(currentTimeSeconds);
        }
        this.replicatorMetrics.get(currentTimeSeconds).get(Counters.APPLIER_TASKS_SUCCEEDED).increment();
    }
}
