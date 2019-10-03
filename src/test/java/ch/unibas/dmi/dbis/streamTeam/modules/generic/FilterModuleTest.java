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

import ch.unibas.dmi.dbis.streamTeam.JUnitTestHelper;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.DummyStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Test class for the FilterModule.
 */
public class FilterModuleTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests if STREAMNAME filter works properly.
     */
    @Test
    public void testStreamNameFilter() throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.EqualityFilter filter = new FilterModule.EqualityFilter(new Schema("streamName"), "dummy");
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements1.get(0), input1);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements2.get(0), input2);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements3.get(0), input3);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(0, outputDataStreamElements4.size());
    }

    /**
     * Tests if KEY filter works properly.
     */
    @Test
    public void testKeyFilter() throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.EqualityFilter filter = new FilterModule.EqualityFilter(new Schema("key"), "key");
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements1.get(0), input1);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements2.get(0), input2);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements4.get(0), input4);
    }

    /**
     * Tests if ARRAYVALUE (not in payload) filter works properly.
     */
    @Test
    public void testArrayValueFilter() throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.EqualityFilter filter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), "objId2");
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(0, outputDataStreamElements1.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements2.get(0), input2);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements3.get(0), input3);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements4.get(0), input4);
    }

    /**
     * Tests if FIELDVALUE (in payload) filter works properly.
     */
    @Test
    public void testPayloadFieldValueFilter() throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.EqualityFilter filter = new FilterModule.EqualityFilter(new Schema("fieldValue{stringValue,true}"), "streamTeamRules");
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(0, outputDataStreamElements1.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements2.get(0), input2);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());
    }

    /**
     * Tests if inequality filter works properly.
     */
    @Test
    public void testInequalityFilter() throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.InequalityFilter filter = new FilterModule.InequalityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), "objId2");
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements1.get(0), input1);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(0, outputDataStreamElements2.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(0, outputDataStreamElements4.size());
    }

    /**
     * Tests if contained in set filter works properly.
     */
    @Test
    public void testContainedInSetFilter() throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Set<Serializable> set = new HashSet<>();
        set.add("objId1");
        set.add("objId3");
        FilterModule.ContainedInSetFilter filter = new FilterModule.ContainedInSetFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), set);
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();
        AbstractImmutableDataStreamElement input5 = createDummyStreamElement4();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements1.get(0), input1);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(0, outputDataStreamElements2.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(0, outputDataStreamElements4.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements5 = module.processElement(input5);
        Assert.assertEquals(1, outputDataStreamElements5.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements5.get(0), input5);
    }

    /**
     * Tests if AND combination is performed properly.
     */
    @Test
    public void testAnd() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotRetrieveInformationException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.EqualityFilter filter1 = new FilterModule.EqualityFilter(new Schema("key"), "key");
        FilterModule.EqualityFilter filter2 = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), "objId2");
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter1, filter2);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(0, outputDataStreamElements1.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements2.get(0), input2);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements4.get(0), input4);
    }

    /**
     * Tests if OR combination is performed properly.
     */
    @Test
    public void testOr() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotRetrieveInformationException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        FilterModule.EqualityFilter filter1 = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), "objId1");
        FilterModule.EqualityFilter filter2 = new FilterModule.EqualityFilter(new Schema("arrayValue{groupIdentifiers,0,false}"), "groupId3");
        FilterModule module = new FilterModule(FilterModule.CombinationType.OR, filter1, filter2);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();
        AbstractImmutableDataStreamElement input5 = createDummyStreamElement4();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements1.get(0), input1);

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(0, outputDataStreamElements2.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(0, outputDataStreamElements4.size());

        List<AbstractImmutableDataStreamElement> outputDataStreamElements5 = module.processElement(input5);
        Assert.assertEquals(1, outputDataStreamElements5.size());
        JUnitTestHelper.assertDataStreamElement(outputDataStreamElements5.get(0), input5);
    }

    /**
     * Tests if a schema of type NO properly throws an exception when apply is called.
     */
    @Test
    public void testExceptionOnFilteringByDoubleValue() throws Schema.SchemaException, FilterModule.FilterException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        this.exception.expect(FilterModule.FilterException.class);

        FilterModule.EqualityFilter filter = new FilterModule.EqualityFilter(new Schema("fieldValue{doubleValue,true}"), 42.21d);
        FilterModule module = new FilterModule(FilterModule.CombinationType.AND, filter);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();

        module.processElement(input1);
    }

    /**
     * Create first dummy stream element for filter module tests.
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

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key", null, null, null, 1L, objectIdentifiers, groupIdentifiers, positions, 1337, true, "helloWorld", 42.21d, repeatedValues, true, null, null, null);
    }

    /**
     * Create second dummy stream element for filter module tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement2() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId2");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId2");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(4.0d, 5.0d, 6.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(200L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1338, false, "streamTeamRules", 43.24d, repeatedValues, true, null, null, null);
    }

    /**
     * Create third dummy stream element for filter module tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement3() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId2");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId2");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(7.0d, 8.0d, 9.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(300L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key2", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1339, false, "yeah", 12.34d, repeatedValues, true, null, null, null);
    }

    /**
     * Create fourth dummy stream element for filter module tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement4() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId3");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId3");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(10.0d, 11.0d, 12.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(400L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1340, false, "beep", 78.90d, repeatedValues, true, null, null, null);
    }

    /**
     * Create otherDummy stream element for filter module tests.
     *
     * @return dummy stream element
     */
    private static AbstractImmutableDataStreamElement createOtherDummyStreamElement() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId2");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId2");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(13.0d, 14.0d, 15.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(500L);

        return DummyStreamElement.generateDummyStreamElement("otherDummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1340, false, "beep", 78.90d, repeatedValues, true, null, null, null);
    }
}