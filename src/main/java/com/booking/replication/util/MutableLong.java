package com.booking.replication.util;

/**
 * Created by bosko on 12/24/15.
 */
public class MutableLong {

    private long value = 0;

    public void increment () {
        value++;
    }

    public long getValue() {
        return value;
    }
}
