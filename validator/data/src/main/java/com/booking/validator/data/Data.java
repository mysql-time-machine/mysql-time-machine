package com.booking.validator.data;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by psalimov on 9/5/16.
 */
public class Data {

    public static DataDiscrepancy discrepancy(Data d0, Data d1){

        if ( d0 == null && d1 == null ) return null;
        if ( d0 == null || d1 == null ) return new DataDiscrepancy();

        return d0.rows.equals(d1.rows) ? null : new DataDiscrepancy();

    }

    private final Map<String,String> rows;

    public Data(Map<String,String> rows){
        this.rows = new HashMap<>(rows);
    }




}
