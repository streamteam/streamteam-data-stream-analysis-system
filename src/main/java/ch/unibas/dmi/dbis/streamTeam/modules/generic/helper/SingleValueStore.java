/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.streamTeam.modules.generic.helper;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * State abstraction for storing a single value.
 * Separated per data stream element key (e.g., matchId).
 * Separated per inner key that can deduced from a data stream element by applying a schema (innerKeySchema) (e.g., first object identifier).
 */
public class SingleValueStore<T extends Serializable> {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(SingleValueStore.class);

    /**
     * Samza key-value store
     */
    private final KeyValueStore<String, Serializable> kvStore;

    /**
     * Key of the single value store
     */
    protected final String singleValueStoreKey;

    /**
     * Schema that is applied to a data stream element to deduce the inner key
     */
    private final Schema innerKeySchema;

    /**
     * SingleValueStore constructor.
     *
     * @param kvStore             Samza Key-Value Store
     * @param singleValueStoreKey Key of the single value store
     * @param innerKeySchema      Schema that is applied to a data stream element to deduce the inner key
     */
    public SingleValueStore(KeyValueStore<String, Serializable> kvStore, String singleValueStoreKey, Schema innerKeySchema) {
        this.kvStore = kvStore;
        this.singleValueStoreKey = singleValueStoreKey;
        this.innerKeySchema = innerKeySchema;
    }

    /**
     * Updates the value of the single value store given a data stream element key and an inner key.
     * Attention: The new value will not be copied!!!
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @param value                New value (WILL NOT BE COPIED!!!)
     */
    public final void put(String dataStreamElementKey, String innerKey, T value) {
        putToKVStore(dataStreamElementKey, innerKey, value);
    }

    /**
     * Updates the value of the single value store given a data stream element.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     * Attention: The new value will not be copied!!!
     *
     * @param dataStreamElement Data stream element
     * @param value             New value (WILL NOT BE COPIED!!!)
     * @throws Schema.SchemaException Thrown if the inner key schema could not be applied.
     */
    public final void put(AbstractImmutableDataStreamElement dataStreamElement, T value) throws Schema.SchemaException {
        String innerKey = this.innerKeySchema.apply(dataStreamElement).toString();
        put(dataStreamElement.getKey(), innerKey, value);
    }

    /**
     * Returns the value of the single value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return Value
     */
    public final T get(String dataStreamElementKey, String innerKey) {
        return getFromKVStore(dataStreamElementKey, innerKey);
    }

    /**
     * Returns the value of the single value store given a data stream element.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @return Value
     * @throws Schema.SchemaException Thrown if the inner key schema could not be applied.
     */
    public final T get(AbstractImmutableDataStreamElement dataStreamElement) throws Schema.SchemaException {
        String innerKey = this.innerKeySchema.apply(dataStreamElement).toString();
        return get(dataStreamElement.getKey(), innerKey);
    }

    /**
     * Returns the long value of the single value store given a data stream element key and an inner key.
     * Returns zero if there is no value stored for the data stream element key and the inner key.
     *
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     * @return Value or zero
     * @throws SingleValueStoreException Thrown if the stored value is no Long.
     */
    public long getLong(String dataStreamElementKey, String innerKey) throws SingleValueStoreException {
        T value = get(dataStreamElementKey, innerKey);
        if (value == null) {
            return 0;
        } else if (Long.class.isInstance(value)) {
            return (Long) value;
        } else {
            throw new SingleValueStoreException("Cannot get Long since the stored value is no Long.");
        }
    }

    /**
     * Returns the long value of the single value store given a data stream element.
     * Returns zero if there is no value stored for the data stream element.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @return Value or zero
     * @throws Schema.SchemaException    Thrown if the inner key schema could not be applied.
     * @throws SingleValueStoreException Thrown if the stored value is no Long.
     */
    public long getLong(AbstractImmutableDataStreamElement dataStreamElement) throws Schema.SchemaException, SingleValueStoreException {
        T value = get(dataStreamElement);
        if (value == null) {
            return 0;
        } else if (Long.class.isInstance(value)) {
            return (Long) value;
        } else {
            throw new SingleValueStoreException("Cannot get Long since the stored value is no Long.");
        }
    }

    /**
     * Returns the double value of the single value store given a data stream element key and an inner key.
     * Returns zero if there is no value stored for the data stream element key and the inner key.
     *
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     * @return Value or zero
     * @throws SingleValueStoreException Thrown if the stored value is no Double.
     */
    public double getDouble(String dataStreamElementKey, String innerKey) throws SingleValueStoreException {
        T value = get(dataStreamElementKey, innerKey);
        if (value == null) {
            return 0.0;
        } else if (Double.class.isInstance(value)) {
            return (Double) value;
        } else {
            throw new SingleValueStoreException("Cannot get Double since the stored value is no Double.");
        }
    }

    /**
     * Returns the double value of the single value store given a data stream element.
     * Returns zero if there is no value stored for the data stream element.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @return Value or zero
     * @throws Schema.SchemaException    Thrown if the inner key schema could not be applied.
     * @throws SingleValueStoreException Thrown if the stored value is no Double.
     */
    public double getDouble(AbstractImmutableDataStreamElement dataStreamElement) throws Schema.SchemaException, SingleValueStoreException {
        T value = get(dataStreamElement);
        if (value == null) {
            return 0.0;
        } else if (Double.class.isInstance(value)) {
            return (Double) value;
        } else {
            throw new SingleValueStoreException("Cannot get Double since the stored value is no Double.");
        }
    }

    /**
     * Returns the boolean value of the single value store given a data stream element key and an inner key.
     * Returns false if there is no value stored for the data stream element key and the inner key.
     *
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     * @return Value or false
     * @throws SingleValueStoreException Thrown if the stored value is no Boolean.
     */
    public boolean getBoolean(String dataStreamElementKey, String innerKey) throws SingleValueStoreException {
        T value = get(dataStreamElementKey, innerKey);
        if (value == null) {
            return false;
        } else if (Boolean.class.isInstance(value)) {
            return (Boolean) value;
        } else {
            throw new SingleValueStoreException("Cannot get Boolean since the stored value is no Boolean.");
        }
    }

    /**
     * Returns the boolean value of the single value store given a data stream element.
     * Returns false if there is no value stored for the data stream element key and the inner key.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @return Value or false
     * @throws Schema.SchemaException    Thrown if the inner key schema could not be applied.
     * @throws SingleValueStoreException Thrown if the stored value is no Boolean.
     */
    public boolean getBoolean(AbstractImmutableDataStreamElement dataStreamElement) throws Schema.SchemaException, SingleValueStoreException {
        T value = get(dataStreamElement);
        if (value == null) {
            return false;
        } else if (Boolean.class.isInstance(value)) {
            return (Boolean) value;
        } else {
            throw new SingleValueStoreException("Cannot get Boolean since the stored value is no Boolean.");
        }
    }

    /**
     * Increases the current value of the single value store given a data stream element key, an inner key, and a value which should be added.
     * Only applicable for Long and Double values.
     *
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     * @param value                Value that will be added to the current value
     * @throws SingleValueStoreException Thrown if the stored value is neither a Long nor a Double.
     */
    public void increase(String dataStreamElementKey, String innerKey, T value) throws SingleValueStoreException {
        if (Long.class.isInstance(value)) {
            long oldValue = this.getLong(dataStreamElementKey, innerKey);
            Long newValue = new Long(oldValue + ((Long) value));
            this.put(dataStreamElementKey, innerKey, (T) newValue);
        } else if (Double.class.isInstance(value)) {
            double oldValue = this.getDouble(dataStreamElementKey, innerKey);
            Double newValue = new Double(oldValue + ((Double) value));
            this.put(dataStreamElementKey, innerKey, (T) newValue);
        } else {
            throw new SingleValueStoreException("Cannot increse the stored value since it is neither a Long nor a Double.");
        }
    }

    /**
     * Increases the current value of the single value store given a data stream element and a value which should be added.
     * Only applicable for Long and Double values.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @param value             Value that will be added to the current value
     * @throws Schema.SchemaException    Thrown if the inner key schema could not be applied.
     * @throws SingleValueStoreException Thrown if the stored value is neither a Long nor a Double.
     */
    public void increase(AbstractImmutableDataStreamElement dataStreamElement, T value) throws Schema.SchemaException, SingleValueStoreException {
        if (Long.class.isInstance(value)) {
            long oldValue = this.getLong(dataStreamElement);
            Long newValue = new Long(oldValue + ((Long) value));
            this.put(dataStreamElement, (T) newValue);
        } else if (Double.class.isInstance(value)) {
            double oldValue = this.getDouble(dataStreamElement);
            Double newValue = new Double(oldValue + ((Double) value));
            this.put(dataStreamElement, (T) newValue);
        } else {
            throw new SingleValueStoreException("Cannot increase value.");
        }
    }

    /**
     * Gets the current value from the Samza key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return Value
     */
    protected T getFromKVStore(String dataStreamElementKey, String innerKey) {
        try {
            return (T) this.kvStore.get("s-" + dataStreamElementKey + "-" + this.singleValueStoreKey + "-" + innerKey);
        } catch (ClassCastException e) {
            logger.error("Caught ClassCastException during getting single value from store for innerKey {}. Returned null instead.", innerKey, e);
            return null;
        }
    }

    /**
     * Puts a new value to the Samza key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @param value                Value
     */
    protected void putToKVStore(String dataStreamElementKey, String innerKey, T value) {
        this.kvStore.put("s-" + dataStreamElementKey + "-" + this.singleValueStoreKey + "-" + innerKey, value);
    }

    /**
     * Indicates there was a problem during reading or writing a value.
     */
    public static class SingleValueStoreException extends Exception {

        /**
         * SingleValueStoreException constructor.
         *
         * @param msg Message that explains the problem
         */
        public SingleValueStoreException(String msg) {
            super(msg);
        }
    }
}
