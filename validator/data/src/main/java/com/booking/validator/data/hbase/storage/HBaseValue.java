package com.booking.validator.data.hbase.storage;

import com.booking.validator.data.storage.Value;
import com.booking.validator.data.Data;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

/**
 * Created by psalimov on 9/8/16.
 */
public class HBaseValue implements Value {

    private final NavigableMap<byte[],byte[]> familyMap;

    public HBaseValue(NavigableMap<byte[],byte[]> familyMap){

        this.familyMap = familyMap;
    }

    @Override
    public Data getData() {

        return new Data( familyMap.entrySet().stream().collect(
                Collectors.toMap( entry -> Bytes.toString( entry.getKey() ), entry -> Bytes.toString( entry.getValue() ) )
            ) );

    }
}
