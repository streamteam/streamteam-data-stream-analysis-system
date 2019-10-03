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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.TestCase.*;

/**
 * Test class for the SingleValueStore(Dummy).
 */
public class SingleValueStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests if get() works properly (with static inner key).
     */
    @Test
    public void testGet() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Integer> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, 0);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 0);

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, 1);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 1);
    }

    /**
     * Tests if getLong() works properly (with static inner key).
     */
    @Test
    public void testGetLong() throws Schema.SchemaException, SingleValueStore.SingleValueStoreException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Long> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        long long1 = singleValueStore.getLong("key", Schema.STATIC_INNER_KEY);
        assertEquals(0L, long1);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, 23L);

        long long2 = singleValueStore.getLong("key", Schema.STATIC_INNER_KEY);
        assertEquals(23L, long2);

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, 0L);

        long long3 = singleValueStore.getLong("key", Schema.STATIC_INNER_KEY);
        assertEquals(0L, long3);
    }

    /**
     * Tests if getDouble() works properly (with static inner key).
     */
    @Test
    public void testGetDouble() throws Schema.SchemaException, SingleValueStore.SingleValueStoreException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Double> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        double double1 = singleValueStore.getDouble("key", Schema.STATIC_INNER_KEY);
        assertEquals(0.0, double1, JUnitTestHelper.DOUBLE_DELTA);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, 23.7);

        double double2 = singleValueStore.getDouble("key", Schema.STATIC_INNER_KEY);
        assertEquals(23.7, double2, JUnitTestHelper.DOUBLE_DELTA);

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, 0.0);

        double double3 = singleValueStore.getDouble("key", Schema.STATIC_INNER_KEY);
        assertEquals(0.0, double3, JUnitTestHelper.DOUBLE_DELTA);
    }

    /**
     * Tests if getBoolean() works properly (with static inner key).
     */
    @Test
    public void testGetBoolean() throws Schema.SchemaException, SingleValueStore.SingleValueStoreException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Boolean> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        boolean boolean1 = singleValueStore.getBoolean("key", Schema.STATIC_INNER_KEY);
        assertFalse(boolean1);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, true);

        boolean boolean2 = singleValueStore.getBoolean("key", Schema.STATIC_INNER_KEY);
        assertTrue(boolean2);

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, false);

        boolean boolean3 = singleValueStore.getBoolean("key", Schema.STATIC_INNER_KEY);
        assertFalse(boolean3);
    }

    /**
     * Tests if increase() works properly for longs (with static inner key).
     */
    @Test
    public void testIncreaseLong() throws Schema.SchemaException, SingleValueStore.SingleValueStoreException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Long> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.increase(input1, 29L);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 29L);

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.increase(input2, -5L);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 24L);

        DummyStreamElement input3 = createDummyStreamElement3();
        singleValueStore.increase(input3, 18L);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 42L);
    }

    /**
     * Tests if increase() works properly for doubles (with static inner key).
     */
    @Test
    public void testIncreaseDouble() throws Schema.SchemaException, SingleValueStore.SingleValueStoreException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Double> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.increase(input1, 29.3);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 29.3);

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.increase(input2, -5.2);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 24.1);

        DummyStreamElement input3 = createDummyStreamElement3();
        singleValueStore.increase(input3, 18.5);

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, 42.6);
    }

    /**
     * Tests if the single value store is properly separated for multiple data stream element keys (with static inner key).
     */
    @Test
    public void testMultipleDataStreamElementKeys() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", Schema.STATIC_INNER_KEY);
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, "1_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, "1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", Schema.STATIC_INNER_KEY);

        DummyStreamElement input4 = createDummyStreamElement4();
        singleValueStore.put(input4, "2_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, "1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", Schema.STATIC_INNER_KEY, "2_1");

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, "1_2");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, "1_2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", Schema.STATIC_INNER_KEY, "2_1");

        DummyStreamElement input3 = createDummyStreamElement3();
        singleValueStore.put(input3, "1_3");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, "1_3");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", Schema.STATIC_INNER_KEY, "2_1");

        DummyStreamElement input5 = createDummyStreamElement5();
        singleValueStore.put(input5, "2_2");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, "1_3");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", Schema.STATIC_INNER_KEY, "2_2");
    }

    /**
     * Tests if the single value store works for lists (with static inner key).
     */
    @Test
    public void testList() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<ArrayList<Integer>> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", Schema.STATIC_INNER_KEY);

        ArrayList<Integer> input1List = new ArrayList<>();
        input1List.add(11);
        input1List.add(12);
        input1List.add(13);
        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, input1List);

        ArrayList<Integer> latestList1 = new ArrayList<>();
        latestList1.add(11);
        latestList1.add(12);
        latestList1.add(13);
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, latestList1);

        ArrayList<Integer> input2List = new ArrayList<>();
        input2List.add(21);
        input2List.add(22);
        input2List.add(23);
        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, input2List);

        ArrayList<Integer> latestList2 = new ArrayList<>();
        latestList2.add(21);
        latestList2.add(22);
        latestList2.add(23);
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, latestList2);
    }

    /**
     * Tests if the single value store works for vectors (with static inner key).
     */
    @Test
    public void testVector() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Geometry.Vector> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.STATIC_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, new Geometry.Vector(1.0, 2.0, 3.0));

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, new Geometry.Vector(1.0, 2.0, 3.0));

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, new Geometry.Vector(4.0, 5.0, 6.0));

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", Schema.STATIC_INNER_KEY, new Geometry.Vector(4.0, 5.0, 6.0));
    }

    /**
     * Tests if the single value store works with a complex innerKeySchema (first object identifier).
     */
    @Test
    public void testComplexInnerKeySchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("arrayValue{objectIdentifiers,0,false}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", schema);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId2");

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, "objId1_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "objId1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId2");

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, "objId2_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "objId1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "objId2_1");

        DummyStreamElement input3 = createDummyStreamElement3();
        singleValueStore.put(input3, "objId2_2");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "objId1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "objId2_2");
    }

    /**
     * Tests if the single value store works for multiple data stream element keys and a complex innerKeySchema (first object identifier).
     */
    @Test
    public void testComplexInnerKeySchemaAndMultipleDataStreamElementKeys() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("arrayValue{objectIdentifiers,0,false}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", schema);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");

        DummyStreamElement input1 = createDummyStreamElement1();
        singleValueStore.put(input1, "1_objId1_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");

        DummyStreamElement input4 = createDummyStreamElement4();
        singleValueStore.put(input4, "2_objId1_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");

        DummyStreamElement input2 = createDummyStreamElement2();
        singleValueStore.put(input2, "1_objId2_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "1_objId2_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");

        DummyStreamElement input3 = createDummyStreamElement3();
        singleValueStore.put(input3, "1_objId2_2");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "1_objId2_2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");

        DummyStreamElement input5 = createDummyStreamElement5();
        singleValueStore.put(input5, "2_objId2_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "1_objId2_2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId2", "2_objId2_1");
    }

    /**
     * Tests if the single value store works with NO_INNER_KEY_SCHEMA as innerKeySchema.
     */
    @Test
    public void testNoInnerKeySchema() {
        String dataStreamElementKey1 = "dataStreamElementKey1";
        String dataStreamElementKey2 = "dataStreamElementKey2";

        String innerKeyA = "innerKeyA";
        String innerKeyB = "innerKeyB";

        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("singleValueStoreKey", Schema.NO_INNER_KEY_SCHEMA);

        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey1, innerKeyA);
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey1, innerKeyB);
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey2, innerKeyA);
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey2, innerKeyB);

        singleValueStore.put(dataStreamElementKey1, innerKeyA, "1_A_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey1, innerKeyA, "1_A_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey1, innerKeyB);
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey2, innerKeyA);
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey2, innerKeyB);

        singleValueStore.put(dataStreamElementKey2, innerKeyA, "2_A_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey1, innerKeyA, "1_A_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey1, innerKeyB);
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey2, innerKeyA, "2_A_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey2, innerKeyB);

        singleValueStore.put(dataStreamElementKey1, innerKeyA, "1_A_2");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey1, innerKeyA, "1_A_2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey1, innerKeyB);
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey2, innerKeyA, "2_A_1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey2, innerKeyB);

        singleValueStore.put(dataStreamElementKey2, innerKeyB, "2_B_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey1, innerKeyA, "1_A_2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, dataStreamElementKey1, innerKeyB);
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey2, innerKeyA, "2_A_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey2, innerKeyB, "2_B_1");

        singleValueStore.put(dataStreamElementKey1, innerKeyB, "1_B_1");

        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey1, innerKeyA, "1_A_2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey1, innerKeyB, "1_B_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey2, innerKeyA, "2_A_1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, dataStreamElementKey2, innerKeyB, "2_B_1");
    }

    /**
     * Create first dummy stream element for single value store tests.
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
     * Create second dummy stream element for single value store tests.
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
     * Create second dummy stream element for single value store tests.
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

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1339, false, "yeah", 12.34d, repeatedValues, true, null, null, null);
    }

    /**
     * Create first dummy stream element for single value store tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement4() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId1");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId1");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(10.0d, 11.0d, 12.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(400L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key2", null, null, null, 1L, objectIdentifiers, groupIdentifiers, positions, 1340, true, "helloWorld", 56.78d, repeatedValues, true, null, null, null);
    }

    /**
     * Create second dummy stream element for single value store tests.
     *
     * @return dummy stream element
     */
    private static DummyStreamElement createDummyStreamElement5() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId2");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId2");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(13.0d, 14.0d, 15.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(500L);

        return DummyStreamElement.generateDummyStreamElement("dummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key2", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1341, false, "streamTeamRules", 43.21d, repeatedValues, true, null, null, null);
    }
}