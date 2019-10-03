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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.LinkedList;
import java.util.List;

/**
 * Test class for ObjectInfoFactoryAndModifier.
 */
public class ObjectInfoFactoryAndModifierTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests if an ObjectInfo can be properly created with a position from a history store.
     */
    @Test
    public void testCreateObjectInfoWithPositionFromHistoryStore() throws Schema.SchemaException, ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<Geometry.Vector> positionHistoryStore = new HistoryStoreDummy<>("position", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);

        List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionHistoryStore));
        StoreModule storePositionsModule = new StoreModule(null, historyStoreList, true);

        DummyStreamElement dummyStreamElement1 = createDummyStreamElement1();
        storePositionsModule.processElement(dummyStreamElement1);

        DummyStreamElement dummyStreamElement2 = createDummyStreamElement2();
        storePositionsModule.processElement(dummyStreamElement2);

        DummyStreamElement dummyStreamElement3 = createDummyStreamElement3();
        storePositionsModule.processElement(dummyStreamElement3);

        ObjectInfo object1 = ObjectInfoFactoryAndModifier.createObjectInfoWithPositionFromHistoryStore("key", "objId1", "groupId1", positionHistoryStore);
        JUnitTestHelper.assertObjectInfo(object1, "objId1", "groupId1", new Geometry.Vector(1.0, 2.0, 3.0), null, null);

        ObjectInfo object2 = ObjectInfoFactoryAndModifier.createObjectInfoWithPositionFromHistoryStore("key", "objId2", "groupId2", positionHistoryStore);
        JUnitTestHelper.assertObjectInfo(object2, "objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), null, null);
    }

    /**
     * Tests if the position of an ObjectInfo can be properly updated with a position from a history store.
     */
    @Test
    public void testUpdateObjectInfoPositionWithHistoryStore() throws Schema.SchemaException, ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<Geometry.Vector> positionHistoryStore = new HistoryStoreDummy<>("position", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);

        List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionHistoryStore));
        StoreModule storePositionsModule = new StoreModule(null, historyStoreList, true);

        DummyStreamElement dummyStreamElement1 = createDummyStreamElement1();
        storePositionsModule.processElement(dummyStreamElement1);

        DummyStreamElement dummyStreamElement2 = createDummyStreamElement2();
        storePositionsModule.processElement(dummyStreamElement2);

        DummyStreamElement dummyStreamElement3 = createDummyStreamElement3();
        storePositionsModule.processElement(dummyStreamElement3);

        ObjectInfo object1 = new ObjectInfo("objId1", "groupId1");
        ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithHistoryStore(object1, "key", positionHistoryStore);
        JUnitTestHelper.assertObjectInfo(object1, "objId1", "groupId1", new Geometry.Vector(1.0, 2.0, 3.0), null, null);

        ObjectInfo object2 = new ObjectInfo("objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(11.0d, 22.0d, 33.0d), 44.0d);
        ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithHistoryStore(object2, "key", positionHistoryStore);
        JUnitTestHelper.assertObjectInfo(object2, "objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(11.0d, 22.0d, 33.0d), 44.0d);
    }

    /**
     * Tests if the velocity of an ObjectInfo can be properly updated with velocity from a history stores.
     */
    @Test
    public void testUpdateObjectInfoVelocityWithHistoryStores() throws Schema.SchemaException, ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        HistoryStore<Double> vxHistoryStore = new HistoryStoreDummy<>("vx", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);
        HistoryStore<Double> vyHistoryStore = new HistoryStoreDummy<>("vy", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);
        HistoryStore<Double> vzHistoryStore = new HistoryStoreDummy<>("vz", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);
        HistoryStore<Double> vabsHistoryStore = new HistoryStoreDummy<>("vabs", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);

        List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vxHistoryStore));
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vyHistoryStore));
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vzHistoryStore));
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vabsHistoryStore));
        StoreModule storePositionsModule = new StoreModule(null, historyStoreList, true);

        DummyStreamElement dummyStreamElement1 = createDummyStreamElement1();
        storePositionsModule.processElement(dummyStreamElement1);

        DummyStreamElement dummyStreamElement2 = createDummyStreamElement2();
        storePositionsModule.processElement(dummyStreamElement2);

        DummyStreamElement dummyStreamElement3 = createDummyStreamElement3();
        storePositionsModule.processElement(dummyStreamElement3);

        ObjectInfo object1 = new ObjectInfo("objId1", "groupId1");
        ObjectInfoFactoryAndModifier.updateObjectInfoVelocityWithHistoryStores(object1, "key", vxHistoryStore, vyHistoryStore, vzHistoryStore, vabsHistoryStore);
        JUnitTestHelper.assertObjectInfo(object1, "objId1", "groupId1", null, new Geometry.Vector(42.21d, 42.21d, 42.21d), 42.21d);

        ObjectInfo object2 = new ObjectInfo("objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(11.0d, 22.0d, 33.0d), 44.0d);
        ObjectInfoFactoryAndModifier.updateObjectInfoVelocityWithHistoryStores(object2, "key", vxHistoryStore, vyHistoryStore, vzHistoryStore, vabsHistoryStore);
        JUnitTestHelper.assertObjectInfo(object2, "objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(12.34d, 12.34d, 12.34d), 12.34d);
    }

    /**
     * Tests if an ObjectInfo can be properly created with a position from a single value store.
     */
    @Test
    public void testCreateObjectInfoWithPositionFromSingleValueStore() throws Schema.SchemaException, ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Geometry.Vector> positionStore = new SingleValueStoreDummy<>("position", new Schema("arrayValue{objectIdentifiers,0,false}"));

        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
        StoreModule storePositionsModule = new StoreModule(singleValueStoreList, null, true);

        DummyStreamElement dummyStreamElement1 = createDummyStreamElement1();
        storePositionsModule.processElement(dummyStreamElement1);

        DummyStreamElement dummyStreamElement2 = createDummyStreamElement2();
        storePositionsModule.processElement(dummyStreamElement2);

        DummyStreamElement dummyStreamElement3 = createDummyStreamElement3();
        storePositionsModule.processElement(dummyStreamElement3);

        ObjectInfo object1 = ObjectInfoFactoryAndModifier.createObjectInfoWithPositionFromSingleValueStore("key", "objId1", "groupId1", positionStore);
        JUnitTestHelper.assertObjectInfo(object1, "objId1", "groupId1", new Geometry.Vector(1.0, 2.0, 3.0), null, null);

        ObjectInfo object2 = ObjectInfoFactoryAndModifier.createObjectInfoWithPositionFromSingleValueStore("key", "objId2", "groupId2", positionStore);
        JUnitTestHelper.assertObjectInfo(object2, "objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), null, null);
    }

    /**
     * Tests if the position of an ObjectInfo can be properly updated with a position from a single value store.
     */
    @Test
    public void testUpdateObjectInfoPositionWithSingleValueStore() throws Schema.SchemaException, ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Geometry.Vector> positionStore = new SingleValueStoreDummy<>("position", new Schema("arrayValue{objectIdentifiers,0,false}"));

        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
        StoreModule storePositionsModule = new StoreModule(singleValueStoreList, null, true);

        DummyStreamElement dummyStreamElement1 = createDummyStreamElement1();
        storePositionsModule.processElement(dummyStreamElement1);

        DummyStreamElement dummyStreamElement2 = createDummyStreamElement2();
        storePositionsModule.processElement(dummyStreamElement2);

        DummyStreamElement dummyStreamElement3 = createDummyStreamElement3();
        storePositionsModule.processElement(dummyStreamElement3);

        ObjectInfo object1 = new ObjectInfo("objId1", "groupId1");
        ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(object1, "key", positionStore);
        JUnitTestHelper.assertObjectInfo(object1, "objId1", "groupId1", new Geometry.Vector(1.0, 2.0, 3.0), null, null);

        ObjectInfo object2 = new ObjectInfo("objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(11.0d, 22.0d, 33.0d), 44.0d);
        ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(object2, "key", positionStore);
        JUnitTestHelper.assertObjectInfo(object2, "objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(11.0d, 22.0d, 33.0d), 44.0d);
    }

    /**
     * Tests if the velocity of an ObjectInfo can be properly updated with velocity from a history stores.
     */
    @Test
    public void testUpdateObjectInfoVelocityWithSingleValueStore() throws Schema.SchemaException, ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        SingleValueStore<Double> vxSingleValueStore = new SingleValueStoreDummy<>("vx", new Schema("arrayValue{objectIdentifiers,0,false}"));
        SingleValueStore<Double> vySingleValueStore = new SingleValueStoreDummy<>("vy", new Schema("arrayValue{objectIdentifiers,0,false}"));
        SingleValueStore<Double> vzSingleValueStore = new SingleValueStoreDummy<>("vz", new Schema("arrayValue{objectIdentifiers,0,false}"));
        SingleValueStore<Double> vabsSingleValueStore = new SingleValueStoreDummy<>("vabs", new Schema("arrayValue{objectIdentifiers,0,false}"));

        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vxSingleValueStore));
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vySingleValueStore));
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vzSingleValueStore));
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{doubleValue,true}"), Double.class, vabsSingleValueStore));
        StoreModule storePositionsModule = new StoreModule(singleValueStoreList, null, true);

        DummyStreamElement dummyStreamElement1 = createDummyStreamElement1();
        storePositionsModule.processElement(dummyStreamElement1);

        DummyStreamElement dummyStreamElement2 = createDummyStreamElement2();
        storePositionsModule.processElement(dummyStreamElement2);

        DummyStreamElement dummyStreamElement3 = createDummyStreamElement3();
        storePositionsModule.processElement(dummyStreamElement3);

        ObjectInfo object1 = new ObjectInfo("objId1", "groupId1");
        ObjectInfoFactoryAndModifier.updateObjectInfoVelocityWithSingleValueStores(object1, "key", vxSingleValueStore, vySingleValueStore, vzSingleValueStore, vabsSingleValueStore);
        JUnitTestHelper.assertObjectInfo(object1, "objId1", "groupId1", null, new Geometry.Vector(42.21d, 42.21d, 42.21d), 42.21d);

        ObjectInfo object2 = new ObjectInfo("objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(11.0d, 22.0d, 33.0d), 44.0d);
        ObjectInfoFactoryAndModifier.updateObjectInfoVelocityWithSingleValueStores(object2, "key", vxSingleValueStore, vySingleValueStore, vzSingleValueStore, vabsSingleValueStore);
        JUnitTestHelper.assertObjectInfo(object2, "objId2", "groupId2", new Geometry.Vector(7.0, 8.0, 9.0), new Geometry.Vector(12.34d, 12.34d, 12.34d), 12.34d);
    }

    /**
     * Create first dummy stream element for object info tests.
     *
     * @return dummy stream element
     */
    public static DummyStreamElement createDummyStreamElement1() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
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
     * Create second dummy stream element for object info tests.
     *
     * @return dummy stream element
     */
    public static DummyStreamElement createDummyStreamElement2() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
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
     * Create second dummy stream element for object info tests.
     *
     * @return dummy stream element
     */
    public static DummyStreamElement createDummyStreamElement3() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
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
}