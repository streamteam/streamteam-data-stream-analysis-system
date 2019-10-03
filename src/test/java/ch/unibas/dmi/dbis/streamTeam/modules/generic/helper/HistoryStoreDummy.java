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

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashMap;

/**
 * Dummy class for testing the HistoryStore.
 * Required since it would be hard to fake the Samza key-value store.
 */
public class HistoryStoreDummy<T extends Serializable> extends HistoryStore<T> {

    /**
     * Dummy key-value store
     */
    private HashMap<String, Serializable> dummyKvStore;

    /**
     * HistoryStoreDummy constructor.
     *
     * @param historyKey     Key of the history
     * @param innerKeySchema Schema that is applied to a data stream element to deduce the inner key
     * @param historyLength  Length of the history
     */
    public HistoryStoreDummy(String historyKey, Schema innerKeySchema, int historyLength) {
        super(null, historyKey, innerKeySchema, historyLength);
        this.dummyKvStore = new HashMap<>();
    }

    /**
     * Gets the deque from the dummy key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return Deque
     */
    @Override
    @SuppressWarnings("unchecked")
    protected ArrayDeque<T> getDequeFromKVStore(String dataStreamElementKey, String innerKey) {
        return (ArrayDeque<T>) this.dummyKvStore.get(dataStreamElementKey + "-" + this.historyKey + "-" + innerKey);
    }

    /**
     * Puts a new deque to the dummy key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @param deque                Deque
     */
    @Override
    protected void putDequeToKVStore(String dataStreamElementKey, String innerKey, ArrayDeque<T> deque) {
        this.dummyKvStore.put(dataStreamElementKey + "-" + this.historyKey + "-" + innerKey, deque);
    }
}
