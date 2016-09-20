package com.booking.validator.service.protocol;

import java.util.Map;

/**
 * Created by psalimov on 9/16/16.
 */
public class DataPointerDescription {

    private static final String STORAGE_TYPE_PROPERTY_NAME = "type";

    private Map<String, String> storage;
    private Map<String, String> key;

    public Map<String, String> getStorage() {
        return storage;
    }

    public Map<String, String> getKey() {
        return key;
    }

    public String getStorageType() {
        return storage.get(STORAGE_TYPE_PROPERTY_NAME);
    }
}
