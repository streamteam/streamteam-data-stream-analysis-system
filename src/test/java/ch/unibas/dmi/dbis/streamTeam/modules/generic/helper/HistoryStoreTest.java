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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Test class for the HistoryStore(Dummy).
 */
public class HistoryStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests if getLatest() works properly (with static inner key).
     */
    @Test
    public void testGetLatest() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<Integer> historyStore = new HistoryStoreDummy<>("historyStoreKey", Schema.STATIC_INNER_KEY_SCHEMA, 3);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, 0);

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, 0);

        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, 1);

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, 1);

        DummyStreamElement input3 = createDummyStreamElement3();
        historyStore.add(input3, 2);

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, 2);
    }

    /**
     * Tests if the length capping works properly (with static inner key).
     */
    @Test
    public void testLengthCapping() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<Integer> historyStore = new HistoryStoreDummy<>("historyStoreKey", Schema.STATIC_INNER_KEY_SCHEMA, 2);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, 0);

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new Integer[]{0}));

        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, 1);

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new Integer[]{1, 0}));

        DummyStreamElement input3 = createDummyStreamElement3();
        historyStore.add(input3, 2);

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new Integer[]{2, 1}));
    }

    /**
     * Tests if the history store is properly separated for multiple data stream element keys (with static inner key).
     */
    @Test
    public void testMultipleDataStreamElementKeys() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<String> historyStore = new HistoryStoreDummy<>("historyStoreKey", Schema.STATIC_INNER_KEY_SCHEMA, 3);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", Schema.STATIC_INNER_KEY);
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, "1_1");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"1_1"}));
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", Schema.STATIC_INNER_KEY);

        DummyStreamElement input4 = createDummyStreamElement4();
        historyStore.add(input4, "2_1");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"2_1"}));

        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, "1_2");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"1_2", "1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"2_1"}));

        DummyStreamElement input3 = createDummyStreamElement3();
        historyStore.add(input3, "1_3");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"1_3", "1_2", "1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"2_1"}));

        DummyStreamElement input5 = createDummyStreamElement5();
        historyStore.add(input5, "2_2");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"1_3", "1_2", "1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", Schema.STATIC_INNER_KEY, Arrays.asList(new String[]{"2_2", "2_1"}));
    }

    /**
     * Tests if the history store works for lists (with static inner key).
     */
    @Test
    public void testListHistory() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<ArrayList<Integer>> historyStore = new HistoryStoreDummy<>("historyStoreKey", Schema.STATIC_INNER_KEY_SCHEMA, 3);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", Schema.STATIC_INNER_KEY);

        ArrayList<Integer> input1List = new ArrayList<>();
        input1List.add(11);
        input1List.add(12);
        input1List.add(13);
        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, input1List);

        ArrayList<Integer> latestList1 = new ArrayList<>();
        latestList1.add(11);
        latestList1.add(12);
        latestList1.add(13);
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, latestList1);

        ArrayList<Integer> input2List = new ArrayList<>();
        input2List.add(21);
        input2List.add(22);
        input2List.add(23);
        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, input2List);

        ArrayList<Integer> latestList2 = new ArrayList<>();
        latestList2.add(21);
        latestList2.add(22);
        latestList2.add(23);
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, latestList2);
    }

    /**
     * Tests if the history store works for vectors (with static inner key).
     */
    @Test
    public void testVector() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<Geometry.Vector> historyStore = new HistoryStoreDummy<>("historyStoreKey", Schema.STATIC_INNER_KEY_SCHEMA, 3);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", Schema.STATIC_INNER_KEY);

        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, new Geometry.Vector(1.0, 2.0, 3.0));

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, new Geometry.Vector(1.0, 2.0, 3.0));

        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, new Geometry.Vector(4.0, 5.0, 6.0));

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", Schema.STATIC_INNER_KEY, new Geometry.Vector(4.0, 5.0, 6.0));
    }

    /**
     * Test if the history store works with a complex innerKeySchema (first object identifier).
     */
    @Test
    public void testComplexInnerKeySchema() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("arrayValue{objectIdentifiers,0,false}");
        HistoryStore<String> historyStore = new HistoryStoreDummy<>("historyStoreKey", schema, 3);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId2");

        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, "objId1_1");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "objId1_1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId2");

        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, "objId2_1");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "objId1_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId2", "objId2_1");

        DummyStreamElement input3 = createDummyStreamElement3();
        historyStore.add(input3, "objId2_2");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "objId1_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId2", "objId2_2");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "objId1", Arrays.asList(new String[]{"objId1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "objId2", Arrays.asList(new String[]{"objId2_2", "objId2_1"}));
    }

    /**
     * Tests if the history store works for multiple data stream element keys and a complex innerKeySchema (first object identifier).
     */
    @Test
    public void testComplexInnerKeySchemaAndMultipleDataStreamElementKeys() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema schema = new Schema("arrayValue{objectIdentifiers,0,false}");
        HistoryStore<String> historyStore = new HistoryStoreDummy<>("historyStoreKey", schema, 3);

        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId2");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId2");

        DummyStreamElement input1 = createDummyStreamElement1();
        historyStore.add(input1, "1_objId1_1");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId2");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId2");

        DummyStreamElement input4 = createDummyStreamElement4();
        historyStore.add(input4, "2_objId1_1");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key", "objId2");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId2");

        DummyStreamElement input2 = createDummyStreamElement2();
        historyStore.add(input2, "1_objId2_1");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId2", "1_objId2_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId2");

        DummyStreamElement input3 = createDummyStreamElement3();
        historyStore.add(input3, "1_objId2_2");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId2", "1_objId2_2");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "objId2");

        DummyStreamElement input5 = createDummyStreamElement5();
        historyStore.add(input5, "2_objId2_1");

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId1", "1_objId1_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key", "objId2", "1_objId2_2");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key2", "objId1", "2_objId1_1");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, "key2", "objId2", "2_objId2_1");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "objId1", Arrays.asList(new String[]{"1_objId1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "objId2", Arrays.asList(new String[]{"1_objId2_2", "1_objId2_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", "objId1", Arrays.asList(new String[]{"2_objId1_1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", "objId2", Arrays.asList(new String[]{"2_objId2_1"}));
    }

    /**
     * Tests if the history store works with NO_INNER_KEY_SCHEMA as innerKeySchema.
     */
    @Test
    public void testNoInnerKeySchema() {
        String dataStreamElementKey1 = "dataStreamElementKey1";
        String dataStreamElementKey2 = "dataStreamElementKey2";

        String innerKey1 = "innerKey1";
        String innerKey2 = "innerKey2";
        String innerKey3 = "innerKey3";

        HistoryStore<String> historyStore = new HistoryStoreDummy<>("historyStoreKey", Schema.NO_INNER_KEY_SCHEMA, 2);

        historyStore.add(dataStreamElementKey1, innerKey1, "a");
        historyStore.add(dataStreamElementKey1, innerKey2, "b");
        historyStore.add(dataStreamElementKey1, innerKey2, "c");
        historyStore.add(dataStreamElementKey2, innerKey3, "d");
        historyStore.add(dataStreamElementKey2, innerKey2, "e");
        historyStore.add(dataStreamElementKey2, innerKey2, "f");
        historyStore.add(dataStreamElementKey2, innerKey1, "g");
        historyStore.add(dataStreamElementKey2, innerKey3, "h");
        historyStore.add(dataStreamElementKey2, innerKey2, "i");
        historyStore.add(dataStreamElementKey1, innerKey2, "j");
        historyStore.add(dataStreamElementKey1, innerKey2, "k");
        historyStore.add(dataStreamElementKey2, innerKey1, "l");
        historyStore.add(dataStreamElementKey2, innerKey3, "m");
        historyStore.add(dataStreamElementKey2, innerKey3, "n");
        historyStore.add(dataStreamElementKey2, innerKey1, "o");
        historyStore.add(dataStreamElementKey1, innerKey2, "p");

        JUnitTestHelper.assertHistoryStoreContent(historyStore, dataStreamElementKey1, innerKey1, Arrays.asList(new String[]{"a"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, dataStreamElementKey1, innerKey2, Arrays.asList(new String[]{"p", "k"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, dataStreamElementKey1, innerKey3, null);
        JUnitTestHelper.assertHistoryStoreContent(historyStore, dataStreamElementKey2, innerKey1, Arrays.asList(new String[]{"o", "l"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, dataStreamElementKey2, innerKey2, Arrays.asList(new String[]{"i", "f"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, dataStreamElementKey2, innerKey3, Arrays.asList(new String[]{"n", "m"}));

        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, dataStreamElementKey1, innerKey1, "a");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, dataStreamElementKey1, innerKey2, "p");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, dataStreamElementKey1, innerKey3, null);
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, dataStreamElementKey2, innerKey1, "o");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, dataStreamElementKey2, innerKey2, "i");
        JUnitTestHelper.assertLatestHistoryStoreElement(historyStore, dataStreamElementKey2, innerKey3, "n");
    }

    /**
     * Create first dummy stream element for history store tests.
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
     * Create second dummy stream element for history store tests.
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
     * Create second dummy stream element for history store tests.
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
     * Create first dummy stream element for history store tests.
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
     * Create second dummy stream element for history store tests.
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