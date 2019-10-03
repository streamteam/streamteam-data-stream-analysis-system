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

package ch.unibas.dmi.dbis.streamTeam.modules.generic;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Module for storing values of data stream elements in single value stores or history stores.
 * If the forwardInputDataStreamElements flag is set to true the module simply forwards every input data stream element (after storing the values).
 */
public class StoreModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(StoreModule.class);

    /**
     * List of single value store list entries each specifying which schema has to be applied to store a value of which class in which single value store
     */
    private final List<SingleValueStoreListEntry> singleValueStoreList;

    /**
     * List of history store list entries each specifying which schema has to be applied to store a value of which class in which history store
     */
    private final List<HistoryStoreListEntry> historyStoreList;

    /**
     * Flag which specifies if input data stream elements are forwarded or not
     */
    private final boolean forwardInputDataStreamElements;

    /**
     * StoreModule constructor.
     *
     * @param singleValueStoreList           List of single value store list entries each specifying which schema has to be applied to store a value of which class in which single value store
     * @param historyStoreList               List of history store list entries each specifying which schema has to be applied to store a value of which class in which history store
     * @param forwardInputDataStreamElements Flag which specifies if input data stream elements are forwarded or not
     */
    public StoreModule(List<SingleValueStoreListEntry> singleValueStoreList, List<HistoryStoreListEntry> historyStoreList, boolean forwardInputDataStreamElements) {
        this.singleValueStoreList = singleValueStoreList;
        this.historyStoreList = historyStoreList;
        this.forwardInputDataStreamElements = forwardInputDataStreamElements;
    }

    /**
     * Stores values of a data stream element in single value stores or history stores.
     * If the forwardInputDataStreamElements flag is set to true the module simply forwards every input data stream element (after storing the values).
     *
     * @param inputDataStreamElement Input data stream element
     * @return Input data stream element if the forwardInputDataStreamElements is set to true
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        try {
            if (this.singleValueStoreList != null) {
                for (SingleValueStoreListEntry singleValueStoreListEntry : this.singleValueStoreList) {
                    addValueToSingleValueStore(inputDataStreamElement, singleValueStoreListEntry);
                }
            }
            if (this.historyStoreList != null) {
                for (HistoryStoreListEntry historyStoreListEntry : this.historyStoreList) {
                    addValueToHistoryStore(inputDataStreamElement, historyStoreListEntry);
                }
            }
        } catch (CannotStoreValueException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        if (this.forwardInputDataStreamElements) {
            outputList.add(inputDataStreamElement);
        }

        return outputList;
    }

    /**
     * Puts a value to a single value store given a data stream element and a SingleValueStoreListEntry.
     *
     * @param dataStreamElement         Data stream element
     * @param singleValueStoreListEntry SingleValueStoreListEntry which specifies which schema has to be applied to store a value of which class in which single value store
     * @throws CannotStoreValueException Thrown if the value cannot be stored
     */
    private void addValueToSingleValueStore(AbstractImmutableDataStreamElement dataStreamElement, SingleValueStoreListEntry singleValueStoreListEntry) throws CannotStoreValueException {
        try {
            Serializable value = singleValueStoreListEntry.schema.apply(dataStreamElement);
            if (singleValueStoreListEntry.clazz.isInstance(value)) {
                if (singleValueStoreListEntry.clazz == String.class || singleValueStoreListEntry.clazz == Double.class || singleValueStoreListEntry.clazz == Integer.class ||
                        singleValueStoreListEntry.clazz == Long.class || singleValueStoreListEntry.clazz == Boolean.class ||
                        singleValueStoreListEntry.clazz == Geometry.Vector.class || singleValueStoreListEntry.clazz == NonAtomicEventPhase.class) {
                    singleValueStoreListEntry.singleValueStore.put(dataStreamElement, value);
                } else {
                    throw new CannotStoreValueException("Cannot add value to single value store: The class (" + singleValueStoreListEntry.clazz + ") is not supported.");
                }
            } else {
                throw new CannotStoreValueException("Cannot add value to single value store: Mismatch between class (" + singleValueStoreListEntry.clazz + ") and value " + value.toString() + ".");
            }
        } catch (Schema.SchemaException e) {
            throw new CannotStoreValueException("Cannot add value to single value store:" + e.toString());
        }
    }

    /**
     * Puts a value to a history store given a data stream element and a HistoryStoreListEntry.
     *
     * @param dataStreamElement     Data stream element
     * @param historyStoreListEntry HistoryStoreListEntry which specifies which schema has to be applied to store a value of which class in which single value store
     * @throws CannotStoreValueException Thrown if the value cannot be stored
     */
    private void addValueToHistoryStore(AbstractImmutableDataStreamElement dataStreamElement, HistoryStoreListEntry historyStoreListEntry) throws CannotStoreValueException {
        try {
            Serializable value = historyStoreListEntry.schema.apply(dataStreamElement);
            if (historyStoreListEntry.clazz.isInstance(value)) {
                if (historyStoreListEntry.clazz == String.class || historyStoreListEntry.clazz == Double.class || historyStoreListEntry.clazz == Integer.class ||
                        historyStoreListEntry.clazz == Long.class || historyStoreListEntry.clazz == Boolean.class ||
                        historyStoreListEntry.clazz == Geometry.Vector.class || historyStoreListEntry.clazz == NonAtomicEventPhase.class) {
                    historyStoreListEntry.historyStore.add(dataStreamElement, value);
                } else {
                    throw new CannotStoreValueException("Cannot add value to single value store: The class (" + historyStoreListEntry.clazz + ") is not supported.");
                }
            } else {
                throw new CannotStoreValueException("Cannot add value to single value store: Mismatch between class (" + historyStoreListEntry.clazz + ") and value " + value.toString() + ".");
            }
        } catch (Schema.SchemaException e) {
            throw new CannotStoreValueException("Cannot add value to single value store: " + e.toString());
        }
    }

    /**
     * Information which schema has to be applied to store a value of which class in which single value store.
     */
    public static class SingleValueStoreListEntry {

        /**
         * The schema which is applied to obtain the value to store
         */
        public final Schema schema;

        /**
         * The class of the value
         */
        public final Class clazz;

        /**
         * The single value store in which the value will be stored
         */
        public final SingleValueStore singleValueStore;

        /**
         * SingleValueStoreListEntry constructor.
         *
         * @param schema           The schema which is applied to obtain the value to store
         * @param clazz            The class of the value
         * @param singleValueStore The single value store in which the value will be stored
         */
        public SingleValueStoreListEntry(Schema schema, Class clazz, SingleValueStore singleValueStore) {
            this.schema = schema;
            this.clazz = clazz;
            this.singleValueStore = singleValueStore;
        }
    }

    /**
     * Information which schema has to be applied to store a value of which class in which history store.
     */
    public static class HistoryStoreListEntry {

        /**
         * The schema which is applied to obtain the value to store
         */
        public final Schema schema;

        /**
         * The class of the value
         */
        public final Class clazz;

        /**
         * The history store in which the value will be stored
         */
        public final HistoryStore historyStore;

        /**
         * HistoryStoreListEntry constructor
         *
         * @param schema       The schema which is applied to obtain the value to store
         * @param clazz        The class of the value
         * @param historyStore The history store in which the value will be stored
         */
        public HistoryStoreListEntry(Schema schema, Class clazz, HistoryStore historyStore) {
            this.schema = schema;
            this.clazz = clazz;
            this.historyStore = historyStore;
        }
    }

    /**
     * Indicates that a value cannot be stored.
     */
    public static class CannotStoreValueException extends Exception {

        /**
         * CannotStoreValueException constructor.
         *
         * @param msg Message that explains the problem
         */
        public CannotStoreValueException(String msg) {
            super(msg);
        }
    }
}
