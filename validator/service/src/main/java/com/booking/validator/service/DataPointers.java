package com.booking.validator.service;


import com.booking.validator.data.DataPointer;
import com.booking.validator.data.DataPointerFactory;
import com.booking.validator.service.protocol.DataPointerDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Created by psalimov on 9/5/16.
 */
public class DataPointers {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointers.class);

    private final Map<String, DataPointerFactory> factories;

    public DataPointers(Map<String, DataPointerFactory> factories) {
        this.factories = factories;
    }

    public DataPointer get(DataPointerDescription description){

        String type = description.getStorageType();

        DataPointerFactory factory = factories.get(type);

        if (factory == null) throw new RuntimeException("No factory for data pointer of the type " + type);

        return factory.produce(description.getStorage(),description.getKey());
    }


}
