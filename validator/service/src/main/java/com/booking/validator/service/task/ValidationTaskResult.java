package com.booking.validator.service.task;

import com.booking.validator.data.DataDiscrepancy;

/**
 * Created by psalimov on 9/7/16.
 */
public class ValidationTaskResult {

    private final DataDiscrepancy dicrepancy;

    public ValidationTaskResult(DataDiscrepancy dicrepancy) {

        this.dicrepancy = dicrepancy;

    }

    public boolean isOk(){

        return  dicrepancy == null;

    }
}
