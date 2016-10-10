package com.booking.validator.service.protocol;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

/**
 * Created by psalimov on 9/16/16.
 */
public class DataPointerDescription {

    private static final String STORAGE_TYPE_PROPERTY_NAME = "type";

    private Map<String, String> storage;

    private Map<String, String> key;

    public DataPointerDescription(){}

    public DataPointerDescription( Map<String, String> storage, Map<String, String> key ){

        this.key = key;

        this.storage = storage;

    }

    public Map<String, String> getStorage() {
        return storage;
    }

    public Map<String, String> getKey() {
        return key;
    }

    @JsonIgnore
    public String getStorageType() {
        return storage.get(STORAGE_TYPE_PROPERTY_NAME);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataPointerDescription that = (DataPointerDescription) o;

        if (storage != null ? !storage.equals(that.storage) : that.storage != null) return false;
        return key != null ? key.equals(that.key) : that.key == null;

    }

    @Override
    public int hashCode() {
        int result = storage != null ? storage.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }
}
