package com.booking.validator.data.hbase.storage;

import com.booking.validator.data.storage.Key;

/**
 * Created by psalimov on 9/8/16.
 */
public class HBaseKey implements Key {

    private final byte[] row;
    private final byte[] family;

    public HBaseKey(byte[] row, byte[] family) {
        this.row = row;
        this.family = family;
    }

    public byte[] row(){
        return row;
    }

    public byte[] family(){
        return family;
    }

}
