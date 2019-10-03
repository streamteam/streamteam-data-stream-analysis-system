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
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Test class for the StoreModule.
 */
public class StoreModuleTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests if storing the STREAMNAME works properly.
     */
    @Test
    public void testStreamNameStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("streamName"), String.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "dummy");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "dummy");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "dummy");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "dummy");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "otherDummy");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "dummy");
    }

    /**
     * Tests if storing the KEY works properly.
     */
    @Test
    public void testKeyStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("key"), String.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "key");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "key");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "key");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "key2");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "key");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "key2");
    }

    /**
     * Tests if storing an ARRAYVALUE (not in payload) works properly.
     */
    @Test
    public void testArrayValueStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "objId2");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId3");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "objId2");
    }

    /**
     * Tests if storing a FIELDVALUE (in payload) works properly.
     */
    @Test
    public void testPayloadFieldValueStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{stringValue,true}"), String.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "helloWorld");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "streamTeamRules");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "streamTeamRules");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "yeah");
    }

    /**
     * Tests if storing a POSITION works properly.
     */
    @Test
    public void testPositionValueStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<Geometry.Vector> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(1.0d, 2.0d, 3.0d));
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(4.0d, 5.0d, 6.0d));
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(4.0d, 5.0d, 6.0d));
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", new Geometry.Vector(7.0d, 8.0d, 9.0d));
    }

    /**
     * Tests if storing multiple values works properly.
     */
    @Test
    public void testMultipleValuesStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<Geometry.Vector> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        SingleValueStore<String> singleValueStore2 = new SingleValueStoreDummy<>("testStorage-2", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, singleValueStore));
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, singleValueStore2));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(1.0d, 2.0d, 3.0d));
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore2, "key", "test", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore2, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(4.0d, 5.0d, 6.0d));
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore2, "key", "test", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore2, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(4.0d, 5.0d, 6.0d));
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore2, "key", "test", "objId2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", new Geometry.Vector(7.0d, 8.0d, 9.0d));
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore2, "key2", "test", "objId2");
    }

    /**
     * Tests if disabled forwarding works properly.
     */
    @Test
    public void testDisabledForwarding() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, false);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(0, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(0, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(0, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "objId2");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(0, outputDataStreamElements4.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", "objId3");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", "objId2");

    }

    /**
     * Tests if storing in history store works properly.
     */
    @Test
    public void testStoringInHistoryStore() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        HistoryStore<String> historyStore = new HistoryStoreDummy<>("testStorage-1", innerKeySchema, 2);
        List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, historyStore));
        StoreModule module = new StoreModule(null, historyStoreList, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId1"}));
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId2", "objId1"}));
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId2", "objId1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", "test", Arrays.asList(new String[]{"objId2"}));

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId3", "objId2"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", "test", Arrays.asList(new String[]{"objId2"}));

    }

    /**
     * Tests if storing in single value store and history store at the same time works properly.
     */
    @Test
    public void testSingleValueAndHistoryStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("static{test}");
        SingleValueStore<Geometry.Vector> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        HistoryStore<String> historyStore = new HistoryStoreDummy<>("testStorage-2", innerKeySchema, 2);

        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, singleValueStore));

        List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
        historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, historyStore));

        StoreModule module = new StoreModule(singleValueStoreList, historyStoreList, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(1.0d, 2.0d, 3.0d));
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId1"}));
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(4.0d, 5.0d, 6.0d));
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "test");
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId2", "objId1"}));
        JUnitTestHelper.assertEmptyHistoryStore(historyStore, "key2", "test");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "test", new Geometry.Vector(4.0d, 5.0d, 6.0d));
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "test", new Geometry.Vector(7.0d, 8.0d, 9.0d));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key", "test", Arrays.asList(new String[]{"objId2", "objId1"}));
        JUnitTestHelper.assertHistoryStoreContent(historyStore, "key2", "test", Arrays.asList(new String[]{"objId2"}));
    }

    /**
     * Tests if storing with complex inner key schema works properly.
     */
    @Test
    public void testComplexInnerKeySchemaStorage() throws Schema.SchemaException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        Schema innerKeySchema = new Schema("arrayValue{objectIdentifiers,0,false}");
        SingleValueStore<String> singleValueStore = new SingleValueStoreDummy<>("testStorage-1", innerKeySchema);
        List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
        singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{groupIdentifiers,0,false}"), String.class, singleValueStore));
        StoreModule module = new StoreModule(singleValueStoreList, null, true);

        AbstractImmutableDataStreamElement input1 = createDummyStreamElement1();
        AbstractImmutableDataStreamElement input2 = createDummyStreamElement2();
        AbstractImmutableDataStreamElement input3 = createDummyStreamElement3();
        AbstractImmutableDataStreamElement input4 = createOtherDummyStreamElement();

        List<AbstractImmutableDataStreamElement> outputDataStreamElements1 = module.processElement(input1);
        Assert.assertEquals(1, outputDataStreamElements1.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "groupId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId3");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId3");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements2 = module.processElement(input2);
        Assert.assertEquals(1, outputDataStreamElements2.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "groupId1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "groupId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId3");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId1");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId3");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements3 = module.processElement(input3);
        Assert.assertEquals(1, outputDataStreamElements3.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "groupId1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "groupId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key", "objId3");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId2", "groupId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId3");

        List<AbstractImmutableDataStreamElement> outputDataStreamElements4 = module.processElement(input4);
        Assert.assertEquals(1, outputDataStreamElements4.size());
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId1", "groupId1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId2", "groupId2");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key", "objId3", "groupId3");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId1");
        JUnitTestHelper.assertSingleValueStoreElement(singleValueStore, "key2", "objId2", "groupId2");
        JUnitTestHelper.assertEmptySingleValueStore(singleValueStore, "key2", "objId3");
    }

    /**
     * Create first dummy stream element for store module tests.
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
     * Create second dummy stream element for store module tests.
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
     * Create third dummy stream element for store module tests.
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
     * Create otherDummy stream element for store module tests.
     *
     * @return dummy stream element
     */
    private static AbstractImmutableDataStreamElement createOtherDummyStreamElement() throws AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<String> objectIdentifiers = new LinkedList<>();
        objectIdentifiers.add("objId3");

        List<String> groupIdentifiers = new LinkedList<>();
        groupIdentifiers.add("groupId3");

        List<Geometry.Vector> positions = new LinkedList<>();
        positions.add(new Geometry.Vector(13.0d, 14.0d, 15.0d));

        List<Long> repeatedValues = new LinkedList<>();
        repeatedValues.add(500L);

        return DummyStreamElement.generateDummyStreamElement("otherDummy", AbstractImmutableDataStreamElement.StreamCategory.EVENT, "key", null, null, null, 2L, objectIdentifiers, groupIdentifiers, positions, 1340, false, "beep", 78.90d, repeatedValues, true, null, null, null);
    }
}