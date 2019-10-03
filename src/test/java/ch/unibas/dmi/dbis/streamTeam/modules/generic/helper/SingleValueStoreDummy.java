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
import java.util.HashMap;

/**
 * Dummy class for testing the SingleValueStore.
 * Required since it would be hard to fake the Samza key-value store.
 */
public class SingleValueStoreDummy<T extends Serializable> extends SingleValueStore<T> {

    /**
     * Dummy key-value store
     */
    private HashMap<String, Serializable> dummyKvStore;

    /**
     * SingleValueStoreDummy constructor.
     *
     * @param singleValueStoreKey Key of the single value store
     * @param innerKeySchema      Schema that is applied to a data stream element to deduce the inner key
     */
    public SingleValueStoreDummy(String singleValueStoreKey, Schema innerKeySchema) {
        super(null, singleValueStoreKey, innerKeySchema);
        this.dummyKvStore = new HashMap<>();
    }

    /**
     * Gets the current value from the dummy key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @return Value
     */
    @Override
    @SuppressWarnings("unchecked")
    protected T getFromKVStore(String dataStreamElementKey, String innerKey) {
        return (T) this.dummyKvStore.get(dataStreamElementKey + "-" + this.singleValueStoreKey + "-" + innerKey);
    }

    /**
     * Puts a new value to the dummy key-value store given a data stream element key and an inner key.
     *
     * @param dataStreamElementKey Key of the data stream element
     * @param innerKey             Inner key
     * @param value                Value
     */
    @Override
    protected void putToKVStore(String dataStreamElementKey, String innerKey, T value) {
        this.dummyKvStore.put(dataStreamElementKey + "-" + this.singleValueStoreKey + "-" + innerKey, value);
    }
}
