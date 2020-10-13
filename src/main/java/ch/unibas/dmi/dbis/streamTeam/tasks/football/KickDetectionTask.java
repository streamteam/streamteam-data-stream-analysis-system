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

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.*;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.KickDetectionModule;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * StreamTask that specifies the analysis logic of the Kick Detection Worker.
 * Consumes fieldObjectState, matchMetadata, ballPossessionChangeEvent, kickoffEvent, areaEvent, and duelEvent and generates kickEvent.
 * Generates a kickEvent for every detected kick.
 */
public class KickDetectionTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(KickDetectionTask.class);

    /**
     * Creates state abstractions and module graphs of the Kick Detection Worker.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs of the Kick Detection Worker.");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            String ballIdentifier = getString(config, "streamTeam.ball");
            double minKickDist = config.getDouble("streamTeam.kickDetection.minKickDist");
            double maxBallbackDist = config.getDouble("streamTeam.kickDetection.maxBallbackDist");

            List<String> playersDefinitionList = config.getList("streamTeam.players");
            List<ObjectInfo> players = new LinkedList<>();
            for (String playerDefinition : playersDefinitionList) {
                players.add(ObjectInfoFactoryAndModifier.createObjectInfoFromPlayerDefinitionString(playerDefinition));
            }

            /*======================================================
            === Create state abstractions                        ===
            ======================================================*/
            SingleValueStore<Geometry.Vector> positionStore = new SingleValueStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore = new SingleValueStore<>(kvStore, "ballPossessionInformation", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<String> leftTeamIdStore = new SingleValueStore<>(kvStore, "leftTeamId", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> currentlyBallInLeftThirdStore = new SingleValueStore<>(kvStore, "currentlyBallInLeftThird", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> currentlyBallInCenterThirdStore = new SingleValueStore<>(kvStore, "currentlyBallInCenterThird", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> currentlyBallInRightThirdStore = new SingleValueStore<>(kvStore, "currentlyBallInRightThird", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Double> fieldLengthStore = new SingleValueStore<>(kvStore, "fieldLength", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<NonAtomicEventPhase> currentDuelPhaseStore = new SingleValueStore<>(kvStore, "currentDuelPhase", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<NonAtomicEventPhase> currentUnderPressurePhaseStore = new SingleValueStore<>(kvStore, "currentUnderPressurePhase", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> activeKickStore = new SingleValueStore<>(kvStore, "activeKick", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // ballFieldObjectStateFilterModule: Forwards only ball fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule.EqualityFilter ballFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule ballFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter, ballFilter);

            // playerFieldObjectStateFilterModule: Forwards only player fieldObjectState stream elements
            FilterModule.InequalityFilter playerFilter = new FilterModule.InequalityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule playerFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter, playerFilter);

            // ballPossessionChangeEventFilterModule: Forwards only ballPossessionChangeEvent stream elements
            FilterModule.EqualityFilter ballPossessionChangeEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), BallPossessionChangeEventStreamElement.STREAMNAME);
            FilterModule ballPossessionChangeEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballPossessionChangeEventStreamNameFilter);

            // kickoffEventFilterModule: Forwards only kickoffEvent stream elements
            FilterModule.EqualityFilter kickoffEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), KickoffEventStreamElement.STREAMNAME);
            FilterModule kickoffEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, kickoffEventStreamNameFilter);

            // ballInLeftThirdFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL" and payload.areaId == "leftThird"
            FilterModule.EqualityFilter areaEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), AreaEventStreamElement.STREAMNAME);
            FilterModule.EqualityFilter ballObjectFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule.EqualityFilter fieldAreaFilterLeft = new FilterModule.EqualityFilter(new Schema("fieldValue{areaId,true}"), "leftThird");
            FilterModule ballInLeftThirdFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, fieldAreaFilterLeft);

            // ballInCenterThirdFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL" and payload.areaId == "centerThird"
            FilterModule.EqualityFilter fieldAreaFilterCenter = new FilterModule.EqualityFilter(new Schema("fieldValue{areaId,true}"), "centerThird");
            FilterModule ballInCenterThirdFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, fieldAreaFilterCenter);

            // ballInRightThirdFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL" and payload.areaId == "rightThird"
            FilterModule.EqualityFilter fieldAreaFilterRight = new FilterModule.EqualityFilter(new Schema("fieldValue{areaId,true}"), "rightThird");
            FilterModule ballInRightThirdFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, fieldAreaFilterRight);

            // duelEventFilterModule: Forwards only duelEvent stream elements
            FilterModule.EqualityFilter duelEventFilter = new FilterModule.EqualityFilter(new Schema("streamName"), DuelEventStreamElement.STREAMNAME);
            FilterModule duelEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, duelEventFilter);

            // underPressureEventFilterModule: Forwards only  underPressureEvent stream elements
            FilterModule.EqualityFilter underPressureEventFilter = new FilterModule.EqualityFilter(new Schema("streamName"), UnderPressureEventStreamElement.STREAMNAME);
            FilterModule underPressureEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, underPressureEventFilter);

            // matchMetadataFilterModule: Forwards only matchMetadata stream elements
            FilterModule.EqualityFilter matchMetadataStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), MatchMetadataStreamElement.STREAMNAME);
            FilterModule matchMetadataFilterModule = new FilterModule(FilterModule.CombinationType.AND, matchMetadataStreamNameFilter);

            // storePlayerFieldObjectStateModule: Stores the latest position for every player
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
            StoreModule storePlayerFieldObjectStateModule = new StoreModule(singleValueStoreList, null, false);

            // storeBallPossessionModule: Stores the information if someone is in ball possession and if yes the playerId and the teamId of the last ballPossessionChangeEvent stream element
            StoreBallPossessionInformationModule storeBallPossessionModule = new StoreBallPossessionInformationModule(ballPossessionInformationStore);

            // storeKickoffEventModule: Stores the identifier of the team which plays on the left side from the last kickoffEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList3 = new LinkedList<>();
            singleValueStoreList3.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{groupIdentifiers,1,false}"), String.class, leftTeamIdStore));
            StoreModule storeKickoffEventModule = new StoreModule(singleValueStoreList3, null, false);

            // kickDetectionModule: Generates a kickEvent stream element when a kick is detected
            KickDetectionModule kickDetectionModule = new KickDetectionModule(players, minKickDist, maxBallbackDist, fieldLengthStore, ballPossessionInformationStore, positionStore, leftTeamIdStore, currentlyBallInLeftThirdStore, currentlyBallInCenterThirdStore, currentlyBallInRightThirdStore, currentDuelPhaseStore, currentUnderPressurePhaseStore, activeKickStore);

            // storeBallInLeftThirdModule: Stores inArea of the last areaEvent stream element with objectIdentifiers[0] == "BALL" and payload.areaId == "leftThird"
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList4 = new LinkedList<>();
            singleValueStoreList4.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{inArea,true}"), Boolean.class, currentlyBallInLeftThirdStore));
            StoreModule storeBallInLeftThirdModule = new StoreModule(singleValueStoreList4, null, false);

            // storeBallInCenterThirdModule: Stores inArea of the last areaEvent stream element with objectIdentifiers[0] == "BALL" and payload.areaId == "centerThird"
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList5 = new LinkedList<>();
            singleValueStoreList5.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{inArea,true}"), Boolean.class, currentlyBallInCenterThirdStore));
            StoreModule storeBallInCenterThirdModule = new StoreModule(singleValueStoreList5, null, false);

            // storeBallInRightThirdModule: Stores inArea of the last areaEvent stream element with objectIdentifiers[0] == "BALL" and payload.areaId == "rightThird"
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList6 = new LinkedList<>();
            singleValueStoreList6.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{inArea,true}"), Boolean.class, currentlyBallInRightThirdStore));
            StoreModule storeBallInRightThirdModule = new StoreModule(singleValueStoreList6, null, false);

            // storeDuelEventPhaseModule: Stores the phase of the last duelEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList7 = new LinkedList<>();
            singleValueStoreList7.add(new StoreModule.SingleValueStoreListEntry(new Schema("phase"), NonAtomicEventPhase.class, currentDuelPhaseStore));
            StoreModule storeDuelEventPhaseModule = new StoreModule(singleValueStoreList7, null, false);

            // storeUnderPressureEventPhaseModule: Stores the phase of the last underPressureEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList8 = new LinkedList<>();
            singleValueStoreList8.add(new StoreModule.SingleValueStoreListEntry(new Schema("phase"), NonAtomicEventPhase.class, currentUnderPressurePhaseStore));
            StoreModule storeUnderPressureEventPhaseModule = new StoreModule(singleValueStoreList8, null, false);

            // matchMetadataStoreModule: Stores the fieldLength from the matchMetadata stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList9 = new LinkedList<>();
            singleValueStoreList9.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{fieldLength,true}"), Double.class, fieldLengthStore));
            StoreModule matchMetadataStoreModule = new StoreModule(singleValueStoreList9, null, false);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickDetectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickDetectionModule, null, "kickDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallFieldObjectStateFilterModule = new LinkedList<>();
            subElementsBallFieldObjectStateFilterModule.add(kickDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballFieldObjectStateFilterModule, subElementsBallFieldObjectStateFilterModule, "ballFieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storePlayerFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storePlayerFieldObjectStateModule, null, "storePlayerFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsPlayerFieldObjectStateFilterModule = new LinkedList<>();
            subElementsPlayerFieldObjectStateFilterModule.add(storePlayerFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement playerFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(playerFieldObjectStateFilterModule, subElementsPlayerFieldObjectStateFilterModule, "playerFieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallPossessionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallPossessionModule, null, "storeBallPossessionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallPossessionChangeEventFilterModuleGraphElement = new LinkedList<>();
            subElementsBallPossessionChangeEventFilterModuleGraphElement.add(storeBallPossessionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionChangeEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionChangeEventFilterModule, subElementsBallPossessionChangeEventFilterModuleGraphElement, "ballPossessionChangeEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeKickoffEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeKickoffEventModule, null, "storeKickoffEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsKickoffEventFilterModule = new LinkedList<>();
            subElementsKickoffEventFilterModule.add(storeKickoffEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickoffEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickoffEventFilterModule, subElementsKickoffEventFilterModule, "kickoffEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallInLeftThirdModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallInLeftThirdModule, null, "storeBallInLeftThirdModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallInLeftThirdFilterModule = new LinkedList<>();
            subElementsBallInLeftThirdFilterModule.add(storeBallInLeftThirdModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballInLeftThirdFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballInLeftThirdFilterModule, subElementsBallInLeftThirdFilterModule, "ballInLeftThirdFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallInCenterThirdModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallInCenterThirdModule, null, "storeBallInCenterThirdModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallInCenterThirdFilterModule = new LinkedList<>();
            subElementsBallInCenterThirdFilterModule.add(storeBallInCenterThirdModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballInCenterThirdFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballInCenterThirdFilterModule, subElementsBallInCenterThirdFilterModule, "ballInCenterThirdFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallInRightThirdModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallInRightThirdModule, null, "storeBallInRightThirdModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallInRightThirdFilterModule = new LinkedList<>();
            subElementsBallInRightThirdFilterModule.add(storeBallInRightThirdModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballInRightThirdFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballInRightThirdFilterModule, subElementsBallInRightThirdFilterModule, "ballInRightThirdFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeDuelEventPhaseModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeDuelEventPhaseModule, null, "storeDuelEventPhaseModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsDuelEventFilterModule = new LinkedList<>();
            subElementsDuelEventFilterModule.add(storeDuelEventPhaseModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement duelEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(duelEventFilterModule, subElementsDuelEventFilterModule, "duelEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeUnderPressureEventPhaseModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeUnderPressureEventPhaseModule, null, "storeUnderPressureEventPhaseModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsUnderPressureEventFilterModule = new LinkedList<>();
            subElementsUnderPressureEventFilterModule.add(storeUnderPressureEventPhaseModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement underPressureEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(underPressureEventFilterModule, subElementsUnderPressureEventFilterModule, "underPressureEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataStoreModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataStoreModule, null, "matchMetadataStoreModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsMatchesFilterModuleGraphElement = new LinkedList<>();
            subElementsMatchesFilterModuleGraphElement.add(matchMetadataStoreModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataFilterModule, subElementsMatchesFilterModuleGraphElement, "matchMetadataFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(ballFieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(playerFieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(ballPossessionChangeEventFilterModuleGraphElement);
            startElementsProcessGraph.add(kickoffEventFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInLeftThirdFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInCenterThirdFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInRightThirdFilterModuleGraphElement);
            startElementsProcessGraph.add(duelEventFilterModuleGraphElement);
            startElementsProcessGraph.add(underPressureEventFilterModuleGraphElement);
            startElementsProcessGraph.add(matchMetadataFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                        +                                           +                                               +                                          +                                            +                                           +                                           +                                          +                                              +                                       +
                        | fieldObjectState,                         | fieldObjectState,                             | fieldObjectState,                        | fieldObjectState,                          | fieldObjectState,                         | fieldObjectState,                         | fieldObjectState,                        | fieldObjectState,                            | fieldObjectState,                     | fieldObjectState,
                        | ballPossessionChangeEvent,                | ballPossessionChangeEvent,                    | ballPossessionChangeEvent,               | ballPossessionChangeEvent,                 | ballPossessionChangeEvent,                | ballPossessionChangeEvent,                | ballPossessionChangeEvent,               | ballPossessionChangeEvent,                   | ballPossessionChangeEvent,            | ballPossessionChangeEvent,
                        | kickoffEvent,                             | kickoffEvent,                                 | kickoffEvent,                            | kickoffEvent,                              | kickoffEvent,                             | kickoffEvent,                             | kickoffEvent,                            | kickoffEvent,                                | kickoffEvent,                         | kickoffEvent,
                        | areaEvent,                                | areaEvent,                                    | areaEvent,                               | areaEvent,                                 | areaEvent,                                | areaEvent,                                | areaEvent,                               | areaEvent,                                   | areaEvent,                            | areaEvent,
                        | duelEvent,                                | duelEvent,                                    | duelEvent,                               | duelEvent,                                 | duelEvent,                                | duelEvent,                                | duelEvent,                               | duelEvent,                                   | duelEvent,                            | duelEvent,
                        | underPressureEvent,                       | underPressureEvent,                           | underPressureEvent,                      | underPressureEvent,                        | underPressureEvent,                       | underPressureEvent,                       | underPressureEvent,                      | underPressureEvent,                          | underPressureEvent,                   | underPressureEvent,
                        | matchMetadata                             | matchMetadata                                 | matchMetadata                            | matchMetadata                              | matchMetadata                             | matchMetadata                             | matchMetadata                            | matchMetadata                                | matchMetadata                         | matchMetadata
            +-----------v--------------------+      +---------------v------------------+          +-----------------v-------------------+          +-----------v------------+                 +-------------v-------------+              +--------------v--------------+              +-------------v--------------+                +----------v----------+                    +--------------v---------------+          +------------+------------+
            |ballFieldObjectStateFilterModule|      |playerFieldObjectStateFilterModule|          |ballPossessionChangeEventFilterModule|          |kickoffEventFilterModule|                 |ballInLeftThirdFilterModule|              |ballInCenterThirdFilterModule|              |ballInRightThirdFilterModule|                |duelEventFilterModule|                    |underPressureEventFilterModule|          |matchMetadataFilterModule|
            +-----------+--------------------+      +---------------+------------------+          +-----------------+-------------------+          +-----------+------------+                 +-------------+-------------+              +--------------+--------------+              +-------------+--------------+                +----------+----------+                    +--------------+---------------+          +------------+------------+
                        |                                           |                                               |                                          |                                            |                                           |                                           |                                          |                                              |                                       |
                        | fieldObjectState                          | fieldObjectState                              | ballPossessionChangeEvent                | kickoffEvent                               | areaEvent                                 | areaEvent                                 | areaEvent                                | duelEvent                                    | underPressureEvent                    | matchMetadata
                        | (only ball)                               | (all but ball)                                |                                          |                                            | (objId = BALL)                            | (objId = BALL)                            | (objId = BALL)                           |                                              |                                       |
                        |                                           |                                               |                                          |                                            | (areaId = leftThird)                      | (areaId = center                          | (areaId = rightThird)                    |                                              |                                       |
               +--------v----------+               +----------------v----------------+                  +-----------v-------------+                +-----------v-----------+                  +-------------v------------+               +--------------v-------------+               +-------------v--------------+              +------------v------------+                +----------------v-----------------+        +------------v-----------+
               |kickDetectionModule|               |storePlayerFieldObjectStateModule|                  |storeBallPossessionModule|                |storeKickoffEventModule|                  |storeBallInLeftThirdModule|               |storeBallInCenterThirdModule|               |storeBallInCenterThirdModule|              |storeDuelEventPhaseModule|                |storeUnderPressureEventPhaseModule|        |matchMetadataStoreModule|
               +--------+----------+               +---------------------------------+                  +-------------------------+                +-----------------------+                  +--------------------------+               +----------------------------+               +----------------------------+              +-------------------------+                +----------------------------------+        +------------------------+
                        |
                        | kickEvent
                        v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
