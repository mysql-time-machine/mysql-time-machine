package com.booking.validator.data.storage;

import com.booking.validator.data.Data;
import com.booking.validator.data.DataPointer;

/**
 * Created by psalimov on 9/15/16.
 */
public class KeyValueStorageDataPointer<K extends Key,V extends Value> implements DataPointer {

    private final KeyValueStorage<K,V> storage;
    private final K key;

    public KeyValueStorageDataPointer(KeyValueStorage<K, V> storage, K key) {
        this.storage = storage;
        this.key = key;
    }

    @Override
    public Data get() {
        return storage.get(key).getData();
    }
}
