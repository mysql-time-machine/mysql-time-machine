package com.booking.replication.util;

/**
 * Created by bosko on 12/24/15.
 */
public class MutableLong {

    private long value;

    public MutableLong() {
        value = 0;
    }

    public MutableLong(long val) {
        value = val;
    }

    public void increment () {
        value++;
    }

    public long getValue() {
        return value;
    }

    public void setValue(Long newValue) { value = newValue; }
}
