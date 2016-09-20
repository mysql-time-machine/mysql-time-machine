package com.booking.validator.service.task;

import com.booking.validator.data.DataPointer;
import com.booking.validator.service.DataPointers;
import com.booking.validator.service.Service;
import com.booking.validator.service.protocol.ValidationTaskDescription;

import java.util.function.Supplier;

/**
 * Created by psalimov on 9/16/16.
 */
public class TaskSupplier implements Supplier<ValidationTask>, Service {


    private final Supplier<ValidationTaskDescription> fetcher;
    private final DataPointers pointers;

    public TaskSupplier(Supplier<ValidationTaskDescription> fetcher, DataPointers pointers) {
        this.fetcher = fetcher;
        this.pointers = pointers;
    }

    @Override
    public void start() {

        if (fetcher instanceof Service) ((Service)fetcher).start();

    }

    @Override
    public ValidationTask get() {

        ValidationTaskDescription description = fetcher.get();

        DataPointer source = pointers.get(description.getSource());
        DataPointer target = pointers.get(description.getTarget());

        return new ValidationTask(source,target);
    }
}
