package com.booking.validator.data.storage;

/**
 * Created by psalimov on 9/5/16.
 */
public interface KeyValueStorage<K extends Key,V extends Value> {

    V get (K key);

}
