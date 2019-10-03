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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Data structure for storing a history of values with a fixed length.
 * Separated per data stream element key (e.g., matchId).
 * Separated per inner key that can deduced from a data stream element by applying a schema (innerKeySchema) (e.g., first object identifier)..
 */
public class HistoryStore<T extends Serializable> {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(HistoryStore.class);

    /**
     * Samza key-value store
     */
    private final KeyValueStore<String, Serializable> kvStore;

    /**
     * Key of the history store
     */
    protected final String historyKey;

    /**
     * Schema that is applied to a data stream element to deduce the inner key
     */
    private final Schema innerKeySchema;

    /**
     * Length of the history
     */
    private final int historyLength;

    /**
     * HistoryStore constructor.
     *
     * @param kvStore        Samza Key-Value Store
     * @param historyKey     Key of the history store
     * @param innerKeySchema Schema that is applied to a data stream element to deduce the inner key
     * @param historyLength  Length of the history
     */
    public HistoryStore(KeyValueStore<String, Serializable> kvStore, String historyKey, Schema innerKeySchema, int historyLength) {
        this.kvStore = kvStore;
        this.historyKey = historyKey;
        this.innerKeySchema = innerKeySchema;
        this.historyLength = historyLength;
    }

    /**
     * Adds a value to the history store given a data stream element key and an inner key.
     * Removes the oldest value in the history store if the history store (for the data stream element key and inner key) is full.
     * Attention: The new value will not be copied!!!
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @param value                New value (WILL NOT BE COPIED!!!)
     */
    public final void add(String dataStreamElementKey, String innerKey, T value) {
        ArrayDeque<T> deque = getDequeFromKVStore(dataStreamElementKey, innerKey);
        if (deque == null) {
            deque = new ArrayDeque<T>(this.historyLength);
        } else if (deque.size() == this.historyLength) {
            deque.removeLast();
        }
        deque.addFirst(value);
        putDequeToKVStore(dataStreamElementKey, innerKey, deque);
    }

    /**
     * Adds a value to the history store given a data stream element.
     * Removes the oldest value in the history store if the history store (for the data stream element key and inner key) is full.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     * Attention: The new value will not be copied!!!
     *
     * @param dataStreamElement Data stream element
     * @param value             New value (WILL NOT BE COPIED!!!)
     * @throws Schema.SchemaException Thrown if the inner key schema could not be applied.
     */
    public final void add(AbstractImmutableDataStreamElement dataStreamElement, T value) throws Schema.SchemaException {
        String innerKey = this.innerKeySchema.apply(dataStreamElement).toString();
        add(dataStreamElement.getKey(), innerKey, value);
    }

    /**
     * Returns the history as a list given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return HistoryStore as a list
     */
    public final List<T> getList(String dataStreamElementKey, String innerKey) {
        ArrayDeque<T> deque = getDequeFromKVStore(dataStreamElementKey, innerKey);
        if (deque == null) {
            return null;
        } else {
            return new ArrayList<T>(deque);
        }
    }

    /**
     * Returns the history as a list given a data stream element.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @return HistoryStore as a list
     * @throws Schema.SchemaException Thrown if the inner key schema could not be applied.
     */
    public final List<T> getList(AbstractImmutableDataStreamElement dataStreamElement) throws Schema.SchemaException {
        String innerKey = this.innerKeySchema.apply(dataStreamElement).toString();
        return getList(dataStreamElement.getKey(), innerKey);
    }

    /**
     * Returns the latest value in the history store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return Latest value
     */
    public final T getLatest(String dataStreamElementKey, String innerKey) {
        ArrayDeque<T> deque = getDequeFromKVStore(dataStreamElementKey, innerKey);
        if (deque == null) {
            return null;
        } else {
            return deque.getFirst();
        }
    }

    /**
     * Returns the latest value in the history store for a data stream element.
     * Automatically deduces the data stream element key and the inner key by means of applying the inner key schema.
     *
     * @param dataStreamElement Data stream element
     * @return Latest value
     * @throws Schema.SchemaException Thrown if the inner key schema could not be applied.
     */
    public final T getLatest(AbstractImmutableDataStreamElement dataStreamElement) throws Schema.SchemaException {
        String innerKey = this.innerKeySchema.apply(dataStreamElement).toString();
        return getLatest(dataStreamElement.getKey(), innerKey);
    }

    /**
     * Gets the deque from the Samza key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return Deque
     */
    protected ArrayDeque<T> getDequeFromKVStore(String dataStreamElementKey, String innerKey) {
        try {
            return (ArrayDeque<T>) this.kvStore.get("h-" + dataStreamElementKey + "-" + this.historyKey + "-" + innerKey);
        } catch (ClassCastException e) {
            logger.error("Caught ClassCastException during getting deque from store for innerKey {}. Returned null instead.", innerKey, e);
            return null;
        }
    }

    /**
     * Puts a new deque to the Samza key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @param deque                Deque
     */
    protected void putDequeToKVStore(String dataStreamElementKey, String innerKey, ArrayDeque<T> deque) {
        this.kvStore.put("h-" + dataStreamElementKey + "-" + this.historyKey + "-" + innerKey, deque);
    }
}
