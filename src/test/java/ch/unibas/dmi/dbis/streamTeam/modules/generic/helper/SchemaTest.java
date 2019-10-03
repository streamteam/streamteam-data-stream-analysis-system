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

import ch.unibas.dmi.dbis.streamTeam.JUnitTestHelper;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.DummyStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.LinkedList;
import java.util.List;

/**
 * Test class for the Schema.
 */
public class SchemaTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests KEY schema.
     */
    @Test
    public void testKeySchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("key");

        DummyStreamElement dse = createDummyStreamElement1();
        Assert.assertEquals("key1", schema.apply(dse));

        dse = createDummyStreamElement2();
        Assert.assertEquals("key2", schema.apply(dse));
    }

    /**
     * Tests STREAMNAME schema.
     */
    @Test
    public void testStreamNameSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("streamName");

        DummyStreamElement dse = createDummyStreamElement1();
        Assert.assertEquals("dummy", schema.apply(dse));

        dse = createDummyStreamElement2();
        Assert.assertEquals("dummy", schema.apply(dse));
    }

    /**
     * Tests STATIC schema.
     */
    @Test
    public void testStaticSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema1 = new Schema("static{staticValue1}");
        Schema schema2 = new Schema("static{staticValue2}");

        DummyStreamElement dse = createDummyStreamElement1();
        Assert.assertEquals("staticValue1", schema1.apply(dse));
        Assert.assertEquals("staticValue2", schema2.apply(dse));

        dse = createDummyStreamElement2();
        Assert.assertEquals("staticValue1", schema1.apply(dse));
        Assert.assertEquals("staticValue2", schema2.apply(dse));
    }

    /**
     * Tests FIELDVALUE schema.
     */
    @Test
    public void testFieldValueSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema1 = new Schema("fieldValue{atomic,false}");
        Schema schema2 = new Schema("fieldValue{generationTimestamp,false}");
        Schema schema3 = new Schema("fieldValue{longValue,true}");
        Schema schema4 = new Schema("fieldValue{boolValue,true}");
        Schema schema5 = new Schema("fieldValue{stringValue,true}");
        Schema schema6 = new Schema("fieldValue{doubleValue,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        Assert.assertEquals(true, schema1.apply(dse));
        Assert.assertEquals(1L, schema2.apply(dse));
        Assert.assertEquals(1337L, schema3.apply(dse));
        Assert.assertEquals(true, schema4.apply(dse));
        Assert.assertEquals("helloWorld", schema5.apply(dse));
        Assert.assertEquals(42.21d, (Double) schema6.apply(dse), JUnitTestHelper.DOUBLE_DELTA);

        dse = createDummyStreamElement2();
        Assert.assertEquals(false, schema1.apply(dse));
        Assert.assertEquals(2L, schema2.apply(dse));
        Assert.assertEquals(1338L, schema3.apply(dse));
        Assert.assertEquals(false, schema4.apply(dse));
        Assert.assertEquals("streamTeamRules", schema5.apply(dse));
        Assert.assertEquals(43.24d, (Double) schema6.apply(dse), JUnitTestHelper.DOUBLE_DELTA);
    }

    /**
     * Tests ARRAYVALUE schema.
     */
    @Test
    public void testArrayValueSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema1a = new Schema("arrayValue{objectIdentifiers,0,false}");
        Schema schema1b = new Schema("arrayValue{objectIdentifiers,1,false}");
        Schema schema2a = new Schema("arrayValue{groupIdentifiers,0,false}");
        //Schema schema2b = new Schema("arrayValue{groupIdentifiers,1,false}");
        Schema schema3a = new Schema("arrayValue{repeatedValue,0,true}");
        Schema schema3b = new Schema("arrayValue{repeatedValue,1,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        Assert.assertEquals("objId1", schema1a.apply(dse));
        // Applying schema1b would result in a CannotApplySchemaException because of an IndexOutOfBoundsException
        Assert.assertEquals("groupId1", schema2a.apply(dse));
        // Applying schema2b would result in a CannotApplySchemaException because of an IndexOutOfBoundsException
        Assert.assertEquals(100L, schema3a.apply(dse));
        // Applying schema3b would result in a CannotApplySchemaException because of an IndexOutOfBoundsException

        dse = createDummyStreamElement2();
        Assert.assertEquals("objId2", schema1a.apply(dse));
        Assert.assertEquals("objId3", schema1b.apply(dse));
        // Applying schema2a and schema2b would result in a CannotApplySchemaException because of an IndexOutOfBoundsException
        Assert.assertEquals(200L, schema3a.apply(dse));
        Assert.assertEquals(300L, schema3b.apply(dse));
    }

    /**
     * Tests ARRAYSIZE schema.
     */
    @Test
    public void testArraySizeSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema1 = new Schema("arraySize{objectIdentifiers,false}");
        Schema schema2 = new Schema("arraySize{groupIdentifiers,false}");
        Schema schema3 = new Schema("arraySize{positions,false}");
        Schema schema4 = new Schema("arraySize{repeatedValue,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        Assert.assertEquals(1, schema1.apply(dse));
        Assert.assertEquals(1, schema2.apply(dse));
        Assert.assertEquals(1, schema3.apply(dse));
        Assert.assertEquals(1, schema4.apply(dse));

        dse = createDummyStreamElement2();
        Assert.assertEquals(2, schema1.apply(dse));
        Assert.assertEquals(0, schema2.apply(dse));
        Assert.assertEquals(2, schema3.apply(dse));
        Assert.assertEquals(2, schema4.apply(dse));
    }

    /**
     * Tests POSITIONVALUE schema.
     */
    @Test
    public void testPositionValueSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema1 = new Schema("positionValue{0}");
        Schema schema2 = new Schema("positionValue{1}");

        DummyStreamElement dse = createDummyStreamElement1();
        Geometry.Vector pos1 = (Geometry.Vector) schema1.apply(dse);
        Assert.assertEquals(1.0d, pos1.x, JUnitTestHelper.DOUBLE_DELTA);
        Assert.assertEquals(2.0d, pos1.y, JUnitTestHelper.DOUBLE_DELTA);
        Assert.assertEquals(3.0d, pos1.z, JUnitTestHelper.DOUBLE_DELTA);
        // Applying schema2 would result in a CannotApplySchemaException because of an IndexOutOfBoundsException

        dse = createDummyStreamElement2();
        Geometry.Vector pos2 = (Geometry.Vector) schema1.apply(dse);
        Assert.assertEquals(4.0d, pos2.x, JUnitTestHelper.DOUBLE_DELTA);
        Assert.assertEquals(5.0d, pos2.y, JUnitTestHelper.DOUBLE_DELTA);
        Assert.assertEquals(6.0d, pos2.z, JUnitTestHelper.DOUBLE_DELTA);
        Geometry.Vector pos3 = (Geometry.Vector) schema2.apply(dse);
        Assert.assertEquals(7.0d, pos3.x, JUnitTestHelper.DOUBLE_DELTA);
        Assert.assertEquals(8.0d, pos3.y, JUnitTestHelper.DOUBLE_DELTA);
        Assert.assertEquals(9.0d, pos3.z, JUnitTestHelper.DOUBLE_DELTA);
    }

    /**
     * Tests PHASE schema.
     */
    @Test
    public void testPhaseSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("phase");

        DummyStreamElement dse = createDummyStreamElement2();
        Assert.assertEquals(NonAtomicEventPhase.ACTIVE, schema.apply(dse));
    }

    /**
     * Tests if a non-existing non-payload FIELDVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnNonexistingNonPayloadField() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("fieldValue{notExisting,false}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a non-existing payload FIELDVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnNonexistingPayloadField() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("fieldValue{notExisting,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a non-existing non-payload ARRAYVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnNonexistingNonPayloadArray() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("arrayValue{notExisting,0,false}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a non-existing payload ARRAYVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnNonexistingPayloadArray() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("arrayValue{notExisting,0,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a too high index of a non-payload ARRAYVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnTooHighIndexNonPayloadArray() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("arrayValue{objectIdentifiers,999,false}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a too high index of a payload ARRAYVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnTooHighIndexPayloadArray() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("arrayValue{repeatedValue,999,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a non-existing non-payload ARRAYSIZE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnNonexistingNonPayloadArraySize() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("arraySize{notExisting,false}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a non-existing payload ARRAYSIZE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnNonexistingPayloadArraySize() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("arraySize{notExisting,true}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a too high index of a POSITIONVALUE schema properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnTooHighIndexPosition() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("positionValue{999}");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a PHASE schema on a non-atomic data stream element properly throws an exception on schema appliance.
     */
    @Test
    public void testExceptionOnPhaseOnNonAtomic() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("phase");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Tests if a schema string that matches no regular expression properly throws an exception on schema creation.
     */
    @Test
    public void testExceptionOnUndefinedCreation() throws Schema.SchemaException {
        this.exception.expect(Schema.SchemaException.class);

        new Schema("anythingUndefined");
    }


    /**
     * Tests if a schema of type NO properly throws an exception when apply is called.
     */
    @Test
    public void testExceptionOnApplyingNoSchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(Schema.SchemaException.class);

        Schema schema = new Schema("no");

        DummyStreamElement dse = createDummyStreamElement1();
        schema.apply(dse);
    }

    /**
     * Create first dummy stream element for schema tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement1() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId1");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId1");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(1.0d, 2.0d, 3.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(100L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key1", null, null, null, 1L, objectIdentifiers, groupIdentifiers, positions, 1337, true, "helloWorld", 42.21d, repeatedValues, true, null, null, null);
    }

    /**
     * Create second dummy stream element for schema tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement2() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId2");
        objectIdentifiers.add("objId3");

        List<String> groupIdentifiers = new LinkedList<>();

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(4.0d, 5.0d, 6.0d));
        positions.add(new Geometry.Vector(7.0d, 8.0d, 9.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(200L);
        repeatedValues.add(300L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key2", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1338, false, "streamTeamRules", 43.24d, repeatedValues, false, NonAtomicEventPhase.ACTIVE, "eventIdentifierInnerKey", 123L);
    }

}