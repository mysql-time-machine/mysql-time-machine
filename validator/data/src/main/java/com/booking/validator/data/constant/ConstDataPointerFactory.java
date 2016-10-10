package com.booking.validator.data.constant;

import com.booking.validator.data.Data;
import com.booking.validator.data.DataPointer;
import com.booking.validator.data.DataPointerFactory;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by psalimov on 9/21/16.
 */
public class ConstDataPointerFactory implements DataPointerFactory {

    public static final String VALUE_PROPERTY_NAME = "value";

    private static class ConstDataPointer implements DataPointer {

        private final Data data;

        private ConstDataPointer(Data data) {
            this.data = data;
        }

        @Override
        public Data get() {
            return data;
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public DataPointer produce(Map<String, String> storageDescription, Map<String, String> keyDescription) throws InvalidDataPointerDescription {

        try {

            Map<String,String> rows = mapper.readValue( keyDescription.get(VALUE_PROPERTY_NAME) , Map.class);

            return new ConstDataPointer(new Data(rows));

        } catch (IOException e) {

            throw new RuntimeException(e);

        }

    }

}
