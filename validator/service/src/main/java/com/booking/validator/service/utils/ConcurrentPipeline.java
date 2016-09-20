package com.booking.validator.service.utils;

import com.booking.validator.service.Service;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by psalimov on 9/7/16.
 *
 * This class implements concurrent execution of the sequence:
 *
 * 1) fetch a task from the supplier
 * 2) execute the task, that is a supplier of a completable future representing its result
 * 3) feed the consumer with the task result or the errorConsumer with the processing error
 *
 * such a way that no more then the specified number of tasks are being processed simultaneously.
 *
 * The supplier, consumer and the errorConsumer must be thread safe.
 */
public class ConcurrentPipeline<T> implements Service {

    private final int concurrencyLimit;
    private final Supplier<? extends Supplier<CompletableFuture<T>>> supplier;
    private final Consumer<T> consumer;
    private final Consumer<Throwable> errorConsumer;

    private volatile boolean run = false;

    public ConcurrentPipeline(Supplier<? extends Supplier<CompletableFuture<T>>> supplier, Consumer<T> consumer, Consumer<Throwable> errorConsumer, int concurrencyLimit) {
        this.supplier = supplier;
        this.consumer = consumer;
        this.concurrencyLimit = concurrencyLimit;
        this.errorConsumer = errorConsumer;
    }

    public void start(){

        run = true;

        for ( int i=0; i<concurrencyLimit; i++ ){
            startTaskAsync();
        }

    }

    private void startTaskAsync(){

        CompletableFuture.supplyAsync(supplier)
                .thenCompose( Supplier::get )
                .whenComplete( this::handleTaskCompletion );

    }

    private void handleTaskCompletion(T result, Throwable throwable ) {

        if (throwable != null){
            errorConsumer.accept( throwable );
        } else {
            consumer.accept(result);
        }

        if (run) startTaskAsync();
    }

}
