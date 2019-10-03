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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Schema for automatically extracting information from a data stream element.
 */
public class Schema {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(Schema.class);

    /**
     * Special schema instance which is inapplicable, i.e., which throws an exception when it is applied.
     * Can be set as the inner key schema of a history or single value store if manual inner key setting in add and get methods is required
     */
    public static final Schema NO_INNER_KEY_SCHEMA;

    /**
     * Special schema instance which always returns the same static value when it is applied.
     * Can be set as the inner key schema of a history or single value store if all data stream elements should be assigned to the same key (and thus only the data stream element key matters)
     */
    public static final Schema STATIC_INNER_KEY_SCHEMA;

    /**
     * The static inner key which is returned by the STATIC_INNER_KEY_SCHEMA
     */
    public static final String STATIC_INNER_KEY;

    /*
     * Initializes the NO_INNER_KEY_SCHEMA, the STATIC_INNER_KEY_SCHEMA, and the STATIC_INNER_KEY
     */
    static {
        STATIC_INNER_KEY = "all";
        Schema noSchema = null;
        Schema staticSchema = null;
        try {
            noSchema = new Schema("no");
            staticSchema = new Schema("static{" + STATIC_INNER_KEY + "}");
        } catch (SchemaException e) {
            logger.error("Caught exception during creating default schemes.", e);
        }
        NO_INNER_KEY_SCHEMA = noSchema;
        STATIC_INNER_KEY_SCHEMA = staticSchema;
    }

    /**
     * Regular expression for an inapplicable schema (used by NO_INNER_KEY_SCHEMA)
     */
    private static final String REGEX_NO = "no";

    /**
     * Regular expression for a schema which returns a static value (used by STATIC_INNER_KEY_SCHEMA but can be used to instantiate multiple static schema instances which return different static values)
     */
    private static final String REGEX_STATIC = "static\\{(.+)\\}";

    /**
     * Regular expression for a schema which returns a field value of the data stream element
     */
    private static final String REGEX_FIELDVALUE = "fieldValue\\{(.+),(.+)\\}";

    /**
     * Regular expression for a schema which returns an array value of the data stream element
     */
    private static final String REGEX_ARRAYVALUE = "arrayValue\\{(.+),(\\d+),(.+)\\}";

    /**
     * Regular expression for a schema which returns the size of an array of the data stream element
     */
    private static final String REGEX_ARRAYSIZE = "arraySize\\{(.+),(.+)\\}";

    /**
     * Regular expression for a schema which returns a position value of the data stream element
     */
    private static final String REGEX_POSITIONVALUE = "positionValue\\{(\\d+)\\}";

    /**
     * Regular expression for a schema which returns the phase of the data stream element
     */
    private static final String REGEX_PHASE = "phase";

    /**
     * Regular expression for a schema which returns the key of the data stream element
     */
    private static final String REGEX_KEY = "key";

    /**
     * Regular expression for a schema which returns the stream name of the data stream element
     */
    private static final String REGEX_STREAMNAME = "streamName";

    /**
     * The type of the schema.
     */
    private SchemaType schemaType;

    /**
     * String that specifies the schema.
     */
    private String schema;

    /**
     * Static value. Only used if the schema type is STATIC.
     */
    private String staticValue;

    /**
     * Name of the field/array in the data stream element. Only used if the schema type is FIELDVALUE or ARRAYVALUE.
     */
    private String valueName;

    /**
     * Specifies if the field/array is part of the payload or not. Only used if the schema type is FIELDVALUE or ARRAYVALUE.
     */
    private boolean valueInPayload;

    /**
     * Index of the array value. Only used if the schema type is ARRAYVALUE or POSITIONVALUE.
     */
    private int arrayValueIndex;

    /**
     * Schema constructor.
     *
     * @param schema String that specifies the schema.
     * @throws SchemaException Thrown if a Schema string cannot be parsed.
     */
    public Schema(String schema) throws SchemaException {
        this.schema = schema;
        if (schema.matches(REGEX_NO)) { // no
            this.schemaType = SchemaType.NO;
        } else if (schema.matches(REGEX_STATIC)) { // static{$1}
            this.schemaType = SchemaType.STATIC;
            this.staticValue = schema.replaceAll(REGEX_STATIC, "$1");
        } else if (schema.matches(REGEX_FIELDVALUE)) { // fieldValue{$1,$2}
            this.schemaType = SchemaType.FIELDVALUE;
            this.valueName = schema.replaceAll(REGEX_FIELDVALUE, "$1");
            String valueInPayloadString = schema.replaceAll(REGEX_FIELDVALUE, "$2");
            try {
                this.valueInPayload = Boolean.parseBoolean(valueInPayloadString);
            } catch (NumberFormatException e) {
                throw new SchemaException("Cannot parse " + schema + ": Wrong number format.");
            }
        } else if (schema.matches(REGEX_ARRAYVALUE)) { // arrayValue{$1,$2,$3}
            this.schemaType = SchemaType.ARRAYVALUE;
            this.valueName = schema.replaceAll(REGEX_ARRAYVALUE, "$1");
            String arrayValueIndexString = schema.replaceAll(REGEX_ARRAYVALUE, "$2");
            String valueInPayloadString = schema.replaceAll(REGEX_ARRAYVALUE, "$3");
            try {
                this.arrayValueIndex = Integer.parseInt(arrayValueIndexString);
                this.valueInPayload = Boolean.parseBoolean(valueInPayloadString);
            } catch (NumberFormatException e) {
                throw new SchemaException("Cannot parse " + schema + ": Wrong number format.");
            }
        } else if (schema.matches(REGEX_ARRAYSIZE)) { // arraySize{$1,$2}
            this.schemaType = SchemaType.ARRAYSIZE;
            this.valueName = schema.replaceAll(REGEX_ARRAYSIZE, "$1");
            String valueInPayloadString = schema.replaceAll(REGEX_ARRAYSIZE, "$2");
            try {
                this.valueInPayload = Boolean.parseBoolean(valueInPayloadString);
            } catch (NumberFormatException e) {
                throw new SchemaException("Cannot parse " + schema + ": Wrong number format.");
            }
        } else if (schema.matches(REGEX_POSITIONVALUE)) { // positionsValue{$1}
            this.schemaType = SchemaType.POSITIONVALUE;
            String arrayValueIndexString = schema.replaceAll(REGEX_POSITIONVALUE, "$1");
            try {
                this.arrayValueIndex = Integer.parseInt(arrayValueIndexString);
            } catch (NumberFormatException e) {
                throw new SchemaException("Cannot parse " + schema + ": Wrong number format.");
            }
        } else if (schema.matches(REGEX_PHASE)) { // phase
            this.schemaType = SchemaType.PHASE;
        } else if (schema.matches(REGEX_KEY)) { // key
            this.schemaType = SchemaType.KEY;
        } else if (schema.matches(REGEX_STREAMNAME)) { // streamName
            this.schemaType = SchemaType.STREAMNAME;
        } else {
            throw new SchemaException("Cannot parse " + schema + ": Does not match any defined schema.");
        }
    }

    /**
     * Applies a schema.
     *
     * @param dataStreamElement Data stream element
     * @return Result
     * @throws SchemaException Thrown if the schema cannot be applied.
     */
    public Serializable apply(AbstractImmutableDataStreamElement dataStreamElement) throws SchemaException {
        switch (this.schemaType) {
            case NO: // Schema is inapplicable
                throw new SchemaException("There is no schema that could be applied.");
            case STATIC: // Schema returns a static value
                return this.staticValue;
            case FIELDVALUE: // Schema returns a field value of the data stream element
                try {
                    return dataStreamElement.getFieldValueByName(this.valueName, this.valueInPayload);
                } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                    throw new SchemaException("Cannot apply " + this.schema + ": " + e.toString());
                }
            case ARRAYVALUE: // Schema returns an array value of the data stream element
                try {
                    return dataStreamElement.getArrayValueByName(this.valueName, this.arrayValueIndex, this.valueInPayload);
                } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                    throw new SchemaException("Cannot apply " + this.schema + ": " + e.toString());
                }
            case ARRAYSIZE: // Schema returns the size of an array of the data stream element
                try {
                    return dataStreamElement.getArraySizeByName(this.valueName, this.valueInPayload);
                } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                    throw new SchemaException("Cannot apply " + this.schema + ": " + e.toString());
                }
            case POSITIONVALUE: // Schema returns a position value of the data stream element
                try {
                    return dataStreamElement.getPosition(this.arrayValueIndex);
                } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                    throw new SchemaException("Cannot apply " + this.schema + ": " + e.toString());
                }
            case PHASE: // Schema returns the phase of the data stream element
                try {
                    return dataStreamElement.getPhase();
                } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
                    throw new SchemaException("Cannot apply " + this.schema + ": " + e.toString());
                }
            case KEY: // Schema returns the key of the data stream element
                return dataStreamElement.getKey();
            case STREAMNAME: // Schema returns the stream name of the data stream element
                return dataStreamElement.getStreamName();
            default:
                throw new SchemaException("Cannot apply " + this.schema + ": Does not match any defined schema.");
        }
    }

    /**
     * Schema types.
     */
    private enum SchemaType {
        /**
         * Inapplicable schema.
         */
        NO,
        /**
         * Schema returns a static value.
         */
        STATIC,
        /**
         * Schema returns a field value of the data stream element.
         */
        FIELDVALUE,
        /**
         * Schema returns an array value of the data stream element.
         */
        ARRAYVALUE,
        /**
         * Schema returns the size of an array of the data stream element.
         */
        ARRAYSIZE,
        /**
         * Schema returns a position value of the data stream element.
         */
        POSITIONVALUE,
        /**
         * Schema returns the phase of the data stream element.
         */
        PHASE,
        /**
         * Schema returns the key of the data stream element.
         */
        KEY,
        /**
         * Schema returns the stream name of the data stream element.
         */
        STREAMNAME
    }

    /**
     * Indicates that a schema could not have been parsed or applied.
     */
    public static class SchemaException extends Exception {

        /**
         * CannotApplySchemaException constructor.
         *
         * @param msg Message that explains the problem
         */
        public SchemaException(String msg) {
            super(msg);
        }
    }
}
