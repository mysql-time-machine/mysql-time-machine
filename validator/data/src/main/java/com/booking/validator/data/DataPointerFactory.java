package com.booking.validator.data;

import com.booking.validator.data.storage.Key;
import com.booking.validator.data.storage.Value;

import java.util.Map;

/**
 * Created by psalimov on 9/15/16.
 */
public interface DataPointerFactory  {

    class InvalidDataPointerDescription extends RuntimeException {
        public InvalidDataPointerDescription(String message){
            super(message);
        }
    }

    DataPointer produce(Map<String,String> storageDescription, Map<String,String> keyDescription) throws InvalidDataPointerDescription;

}
