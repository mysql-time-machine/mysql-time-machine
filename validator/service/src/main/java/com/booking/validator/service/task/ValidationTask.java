package com.booking.validator.service.task;

import com.booking.validator.data.Data;
import com.booking.validator.data.DataPointer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Created by psalimov on 9/5/16.
 */
public class ValidationTask implements Supplier<CompletableFuture<ValidationTaskResult>> {

    private final DataPointer source;
    private final DataPointer target;


    public ValidationTask(DataPointer source, DataPointer target) {
        this.source = source;
        this.target = target;
    }

    public CompletableFuture<ValidationTaskResult> get(){

        return CompletableFuture.supplyAsync( source )
                .thenCombineAsync(
                        CompletableFuture.supplyAsync( target ), this::validate);

    }

    protected ValidationTaskResult validate(Data sourceData, Data targetData ){

        return new ValidationTaskResult( Data.discrepancy( sourceData,targetData ) );

    }

}
