package com.booking.validator.service;


import com.booking.validator.service.task.ValidationTaskResult;
import com.booking.validator.service.utils.ConcurrentPipeline;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by psalimov on 9/6/16.
 */
public class Validator implements Service {

    private final Supplier<? extends Supplier<CompletableFuture<ValidationTaskResult>>> taskSupplier;
    private final Consumer<ValidationTaskResult> resultConsumer;
    private final Consumer<Throwable> errorConsumer;

    public Validator(Supplier<? extends Supplier<CompletableFuture<ValidationTaskResult>>> taskSupplier, Consumer<ValidationTaskResult> resultConsumer, Consumer<Throwable> errorConsumer) {
        this.taskSupplier = taskSupplier;
        this.resultConsumer = resultConsumer;
        this.errorConsumer = errorConsumer;
    }

    @Override
    public void start() {

        Arrays.asList(resultConsumer,errorConsumer,taskSupplier).forEach( x -> {if (x instanceof Service) ((Service) x).start();}  );

        ConcurrentPipeline<ValidationTaskResult> pipeline = new ConcurrentPipeline<>(taskSupplier, resultConsumer, errorConsumer,16);

        pipeline.start();


    }

}
