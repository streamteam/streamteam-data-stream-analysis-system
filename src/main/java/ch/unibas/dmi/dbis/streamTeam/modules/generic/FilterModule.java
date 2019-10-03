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
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Module for filtering data stream elements.
 * Generates a single or no output data stream element for every input data stream element.
 */
public class FilterModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(FilterModule.class);

    /**
     * Type of the combination of the filter results
     */
    private CombinationType combinationType;

    /**
     * Array of filters
     */
    private FilterInterface[] filterArray;

    /**
     * FilterModule constructor.
     *
     * @param combinationType Type of the combination of the filter results
     * @param filter          Filter
     */
    public FilterModule(CombinationType combinationType, FilterInterface... filter) {
        this.combinationType = combinationType;
        this.filterArray = filter;
    }

    /**
     * Filters data stream elements.
     * Generates a single or no output data stream element for every input data stream element.
     *
     * @param inputDataStreamElement Input data stream element
     * @return Output data stream element
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        try {
            boolean pass = true; // forward the input stream element if there is not a single filter
            FilterLoop:
            for (FilterInterface filter : this.filterArray) {
                boolean resultForCurrentFilter = filter.filter(inputDataStreamElement);
                switch (this.combinationType) {
                    case AND:
                        if (!resultForCurrentFilter) {
                            pass = false;
                            break FilterLoop; // AND --> do not forward if at least one filter does not match
                        } else {
                            pass = true;
                        }
                        break;
                    case OR:
                        if (resultForCurrentFilter) {
                            pass = true;
                            break FilterLoop; // OR --> forward if at least one filter matches
                        } else {
                            pass = false;
                        }
                }
            }

            if (pass) {
                outputList.add(inputDataStreamElement);
            }
        } catch (FilterException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Combination types.
     */
    public enum CombinationType {
        /**
         * Conjunction (and).
         */
        AND,
        /**
         * Disjunction (or).
         */
        OR;
    }

    /**
     * Interface for the Filters.
     */
    public interface FilterInterface {

        /**
         * Checks if a data stream element fulfills a filter.
         *
         * @param dataStreamElement Data stream element
         * @return True if the data stream element fulfills the filter
         * @throws FilterException Thrown if the filter cannot be applied
         */
        boolean filter(AbstractImmutableDataStreamElement dataStreamElement) throws FilterException;
    }

    /**
     * Filter that checks if the result of applying a schema to a data stream element is equal to a given value.
     */
    public static class EqualityFilter implements FilterInterface {

        /**
         * Schema
         */
        private final Schema schema;

        /**
         * Expected value
         */
        private final Serializable expectedValue;

        /**
         * EqualityFilter constructor.
         *
         * @param schema        Schema
         * @param expectedValue Expected value
         * @throws FilterException Thrown if the schema and thus the filter cannot by initialized
         */
        public EqualityFilter(Schema schema, Serializable expectedValue) throws FilterException {
            if (expectedValue instanceof Geometry.Vector || expectedValue instanceof Double || expectedValue instanceof Float) {
                throw new FilterException("Cannot initialize EqualityFilter: No support for floating point numbers.");
            } else {
                this.schema = schema;
                this.expectedValue = expectedValue;
            }
        }

        /**
         * Verifies if the result of applying the schema to the data stream element is equal to the expected value.
         *
         * @param dataStreamElement Data stream element
         * @return True if the result of applying the schema to the data stream element is equal to the expected value
         * @throws FilterException Thrown if the schema and thus the filter cannot by applied
         */
        @Override
        public boolean filter(AbstractImmutableDataStreamElement dataStreamElement) throws FilterException {
            try {
                Serializable value = this.schema.apply(dataStreamElement);
                return value.equals(this.expectedValue);
            } catch (Schema.SchemaException e) {
                throw new FilterException("Cannot check filter: " + e.toString());
            }
        }
    }

    /**
     * Filter that checks if the result of applying a schema to a data stream element is unequal to a given value.
     */
    public static class InequalityFilter implements FilterInterface {

        /**
         * Schema
         */
        private final Schema schema;

        /**
         * Forbidden value
         */
        private final Serializable forbiddenValue;

        /**
         * InequalityFilter constructor.
         *
         * @param schema         Schema
         * @param forbiddenValue Forbidden value
         * @throws FilterException Thrown if the schema and thus the filter cannot by initialized
         */
        public InequalityFilter(Schema schema, Serializable forbiddenValue) throws FilterException {
            if (forbiddenValue instanceof Geometry.Vector || forbiddenValue instanceof Double || forbiddenValue instanceof Float) {
                throw new FilterException("Cannot initialize InequalityFilter: No support for floating point numbers.");
            } else {
                this.schema = schema;
                this.forbiddenValue = forbiddenValue;
            }
        }

        /**
         * Verifies if the result of applying the schema to the data stream element is unequal to the forbidden value.
         *
         * @param dataStreamElement Data stream element
         * @return True if the result of applying the schema to the data stream element is unequal to the forbidden value
         * @throws FilterException Thrown if the schema and thus the filter cannot by applied
         */
        @Override
        public boolean filter(AbstractImmutableDataStreamElement dataStreamElement) throws FilterException {
            try {
                Serializable value = this.schema.apply(dataStreamElement);
                return !value.equals(this.forbiddenValue);
            } catch (Schema.SchemaException e) {
                throw new FilterException("Cannot check filter: " + e.toString());
            }
        }
    }

    /**
     * Filter that checks if the result of applying a schema to a data stream element is contained in a given set
     */
    public static class ContainedInSetFilter implements FilterInterface {

        /**
         * Schema
         */
        private final Schema schema;

        /**
         * Set of values
         */
        private final Set<Serializable> set;

        /**
         * ContainedInSetFilter constructor.
         *
         * @param schema Schema
         * @param set    Set of values
         * @throws FilterException Thrown if the schema and thus the filter cannot by initialized
         */
        public ContainedInSetFilter(Schema schema, Set<Serializable> set) throws FilterException {
            for (Serializable entry : set) {
                if (entry instanceof Geometry.Vector || entry instanceof Double || entry instanceof Float) {
                    throw new FilterException("Cannot initialize ContainedInSetFilter: No support for floating point numbers.");
                }
            }
            this.schema = schema;
            this.set = set;
        }

        /**
         * Verifies if the result of applying the schema to the data stream element is contained in the given set.
         *
         * @param dataStreamElement Data stream element
         * @return True if the result of applying the schema to the data stream element is contained in the given set.
         * @throws FilterException Thrown if the schema and thus the filter cannot by applied
         */
        @Override
        public boolean filter(AbstractImmutableDataStreamElement dataStreamElement) throws FilterException {
            try {
                Serializable value = this.schema.apply(dataStreamElement);
                return this.set.contains(value);
            } catch (Schema.SchemaException e) {
                throw new FilterException("Cannot check filter: " + e.toString());
            }
        }
    }

    /**
     * Indicates that the filter cannot be initialized or applied.
     */
    public static class FilterException extends Exception {

        /**
         * CannotApplyFilterException constructor.
         *
         * @param msg Message that explains the problem
         */
        public FilterException(String msg) {
            super(msg);
        }
    }
}
