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

package ch.unibas.dmi.dbis.streamTeam.tasks.football;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionChangeEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickoffEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.OffsideModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.StoreBallPossessionInformationModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import ch.unibas.dmi.dbis.streamTeam.tasks.AbstractTask;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Task that consumes fieldObjectState, ballPossessionChangeEvent, and kickoffEvent and generates offsideLineState.
 * Generates an offsideLineState stream element for every fieldObjectState stream element of the player in ball possession.
 */
public class OffsideTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(OffsideTask.class);

    /**
     * Initializes OffsideTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize OffsideTask");
        try {
            /*======================================================
            === Read Parameters from config file                 ===
            ======================================================*/
            String ballIdentifier = getString(config, "streamTeam.ball");

            List<String> playersDefinitionList = config.getList("streamTeam.players");
            List<ObjectInfo> players = new LinkedList<>();
            for (String playerDefinition : playersDefinitionList) {
                players.add(ObjectInfoFactoryAndModifier.createObjectInfoFromPlayerDefinitionString(playerDefinition));
            }

            /*======================================================
            === Create Stores                                    ===
            ======================================================*/
            KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) taskContext.getStore("kvStore");

            SingleValueStore<Geometry.Vector> positionStore = new SingleValueStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore = new SingleValueStore<>(kvStore, "ballPossessionInformation", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<String> leftTeamIdStore = new SingleValueStore<>(kvStore, "leftTeamId", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> lastOffsideLineWasNullStore = new SingleValueStore<>(kvStore, "lastOffsideLineWasNull", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // playerFieldObjectStateFilterModule: Forwards only player fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule.InequalityFilter playerFilter = new FilterModule.InequalityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule playerFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter, playerFilter);

            // ballPossessionChangeEventFilterModule: Forwards only ballPossessionChangeEvent stream elements
            FilterModule.EqualityFilter ballPossessionChangeEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), BallPossessionChangeEventStreamElement.STREAMNAME);
            FilterModule ballPossessionChangeEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballPossessionChangeEventStreamNameFilter);

            // kickoffEventFilterModule: Forwards only kickoffEvent stream elements
            FilterModule.EqualityFilter kickoffEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), KickoffEventStreamElement.STREAMNAME);
            FilterModule kickoffEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, kickoffEventStreamNameFilter);

            // storePlayerFieldObjectStateModule: Stores latest position for every player
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
            StoreModule storePlayerFieldObjectStateModule = new StoreModule(singleValueStoreList, null, true);

            // storeBallPossessionModule: Stores the information if someone is in ball possession and if yes the playerId and the teamId of the last ballPossessionChangeEvent stream element
            StoreBallPossessionInformationModule storeBallPossessionModule = new StoreBallPossessionInformationModule(ballPossessionInformationStore);

            // storeKickoffEventModule: Stores the identifier of the team which plays on the left side from the last kickoffEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList3 = new LinkedList<>();
            singleValueStoreList3.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{groupIdentifiers,1,false}"), String.class, leftTeamIdStore));
            StoreModule storeKickoffEventModule = new StoreModule(singleValueStoreList3, null, false);

            // offsideModule: Generates an offsideLineState stream element for every fieldObjectState stream element of the player in ball possession
            OffsideModule offsideModule = new OffsideModule(players, ballPossessionInformationStore, positionStore, leftTeamIdStore, lastOffsideLineWasNullStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement offsideModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(offsideModule, null, "offsideModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStorePlayerFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStorePlayerFieldObjectStateModuleGraphElement.add(offsideModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storePlayerFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storePlayerFieldObjectStateModule, subElementsStorePlayerFieldObjectStateModuleGraphElement, "storePlayerFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsPlayerFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsPlayerFieldObjectStateFilterModuleGraphElement.add(storePlayerFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement playerFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(playerFieldObjectStateFilterModule, subElementsPlayerFieldObjectStateFilterModuleGraphElement, "playerFieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallPossessionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallPossessionModule, null, "storeBallPossessionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallPossessionChangeEventFilterModuleGraphElement = new LinkedList<>();
            subElementsBallPossessionChangeEventFilterModuleGraphElement.add(storeBallPossessionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionChangeEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionChangeEventFilterModule, subElementsBallPossessionChangeEventFilterModuleGraphElement, "ballPossessionChangeEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeKickoffEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeKickoffEventModule, null, "storeKickoffEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsKickoffEventFilterModuleGraphElement = new LinkedList<>();
            subElementsKickoffEventFilterModuleGraphElement.add(storeKickoffEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickoffEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickoffEventFilterModule, subElementsKickoffEventFilterModuleGraphElement, "kickoffEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(playerFieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(ballPossessionChangeEventFilterModuleGraphElement);
            startElementsProcessGraph.add(kickoffEventFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                            +                                               +                                          +
                            | fieldObjectState,                             | fieldObjectState,                        | fieldObjectState,
                            | ballPossessionChangeEvent,                    | ballPossessionChangeEvent,               | ballPossessionChangeEvent,
                            | kickoffEvent                                  | kickoffEvent                             | kickoffEvent
            +---------------v------------------+          +-----------------v-------------------+          +-----------v------------+
            |playerFieldObjectStateFilterModule|          |ballPossessionChangeEventFilterModule|          |kickoffEventFilterModule|
            +---------------+------------------+          +-----------------+-------------------+          +-----------+------------+
                            |                                               |                                          |
                            | fieldObjectState                              | ballPossessionChangeEvent                | kickoffEvent
                            | (all but ball)                                |                                          |
                            |                                               |                                          |
            +---------------v-----------------+                 +-----------v-------------+                +-----------v-----------+
            |storePlayerFieldObjectStateModule|                 |storeBallPossessionModule|                |storeKickoffEventModule|
            +---------------+-----------------+                 +-------------------------+                +-----------------------+
                            |
                            | fieldObjectState
                            | (all but ball)
                            |
                     +------+------+
                     |offsideModule|
                     +------+------+
                            |
                            | offsideLineState
                            v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
