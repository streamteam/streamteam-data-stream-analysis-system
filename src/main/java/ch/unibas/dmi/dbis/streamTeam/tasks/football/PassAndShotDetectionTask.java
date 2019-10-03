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

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.AreaEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionChangeEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickoffEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.PassAndShotDetectionModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.GroupInfoFactory;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Task that consumes kickEvent, kickoffEvent, ballPossessionChangeEvent, and areaEvent and generates successfulPassEvent, interceptionEvent, misplacedPassEvent, clearanceEvent, goalEvent, shotOffTargetEvent, passStatistics, and shotStatistics.
 * Generates a successfulPassEvent for every detected successful pass.
 * Generates an interceptionEvent for every detected interception.
 * Generates a misplacedPassEvent for every detected misplaced pass.
 * Generates a clearanceEvent for every detected clearance.
 * Generates a goalEvent for every detected goal.
 * Generates a shotOffTargetEvent for every detected shot off target.
 * Generates passStatistics stream elements for every detected successful pass, interception, misplaced pass, or clearance.
 * Generates shotStatistics stream elements for every detected goal or shot off target.
 */
public class PassAndShotDetectionTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PassAndShotDetectionTask.class);

    /**
     * Initializes PassAndShotDetectionTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize PassAndShotDetectionTask");
        try {
            /*======================================================
            === Read Parameters from config file                 ===
            ======================================================*/
            String ballIdentifier = getString(config, "streamTeam.ball");
            long maxTime = config.getLong("streamTeam.passAndShotDetection.maxTime");
            double sidewardsAngleThreshold = config.getDouble("streamTeam.passAndShotDetection.sidewardsAngleThreshold");
            double goalHeight = config.getDouble("streamTeam.passAndShotDetection.goalHeight");

            List<String> playersDefinitionList = config.getList("streamTeam.players");
            List<ObjectInfo> players = new LinkedList<>();
            for (String playerDefinition : playersDefinitionList) {
                players.add(ObjectInfoFactoryAndModifier.createObjectInfoFromPlayerDefinitionString(playerDefinition));
            }

            List<String> teamsDefinitionList = config.getList("streamTeam.teams");
            List<GroupInfo> teams = new LinkedList<>();
            for (String teamDefinition : teamsDefinitionList) {
                teams.add(GroupInfoFactory.createGroupInfoFromTeamDefinitionString(teamDefinition));
            }

            /*======================================================
            === Create Stores                                    ===
            ======================================================*/
            KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) taskContext.getStore("kvStore");

            SingleValueStore<Long> kickTsStore = new SingleValueStore<>(kvStore, "kickTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> kickPlayerIdStore = new SingleValueStore<>(kvStore, "kickPlayerId", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> kickTeamIdStore = new SingleValueStore<>(kvStore, "kickTeamId", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Geometry.Vector> kickPlayerPosStore = new SingleValueStore<>(kvStore, "kickPlayerPos", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Integer> kickNumPlayerNearerToGoalStore = new SingleValueStore<>(kvStore, "kickNumPlayerNearerToGoal", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> kickAttackedStore = new SingleValueStore<>(kvStore, "kickAttacked", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> kickZoneStore = new SingleValueStore<>(kvStore, "kickZone", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<String> leftTeamIdStore = new SingleValueStore<>(kvStore, "leftTeamId", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> currentlyBallInLeftThirdStore = new SingleValueStore<>(kvStore, "currentlyBallInLeftThird", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> currentlyBallInCenterThirdStore = new SingleValueStore<>(kvStore, "currentlyBallInCenterThird", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> currentlyBallInRightThirdStore = new SingleValueStore<>(kvStore, "currentlyBallInRightThird", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Long> lastUsedKickEventTsStore = new SingleValueStore<>(kvStore, "lastUsedKickEventTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numSuccessfulPassesStore = new SingleValueStore<>(kvStore, "numSuccessfulPasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numInterceptionsStore = new SingleValueStore<>(kvStore, "numInterceptions", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numMisplacedPassesStore = new SingleValueStore<>(kvStore, "numMisplacedPasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numClearancesStore = new SingleValueStore<>(kvStore, "numClearances", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numForwardPassesStore = new SingleValueStore<>(kvStore, "numForwardPasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numBackwardPassesStore = new SingleValueStore<>(kvStore, "numBackwardPasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numLeftPassesStore = new SingleValueStore<>(kvStore, "numLeftPasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numRightPassesStore = new SingleValueStore<>(kvStore, "numRightPasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> sumPackingStore = new SingleValueStore<>(kvStore, "sumPacking", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numShotsOffTargetStore = new SingleValueStore<>(kvStore, "numShotsOffTarget", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numGoalsStore = new SingleValueStore<>(kvStore, "numGoals", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore = new SingleValueStore<>(kvStore, "hasSentInitialStatisticsStreamElements", Schema.NO_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // kickEventFilterModule: Forwards only kickEvent stream elements
            FilterModule.EqualityFilter kickEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), KickEventStreamElement.STREAMNAME);
            FilterModule kickEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, kickEventStreamNameFilter);

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

            // ballInRightThirdFilterModule:Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL" and payload.areaId == "rightThird"
            FilterModule.EqualityFilter fieldAreaFilterRight = new FilterModule.EqualityFilter(new Schema("fieldValue{areaId,true}"), "rightThird");
            FilterModule ballInRightThirdFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, fieldAreaFilterRight);

            // ballPossessionChangeEventFilterModule: Forwards only ballPossessionChangeEvent stream elements
            FilterModule.EqualityFilter ballPossessionChangeEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), BallPossessionChangeEventStreamElement.STREAMNAME);
            FilterModule ballPossessionChangeEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballPossessionChangeEventStreamNameFilter);

            // secondEventAreaEventFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL" and payload.areaId value in a given set
            Set<Serializable> setOfAreaIds = new HashSet<>();
            setOfAreaIds.add("aboveLeftThird");
            setOfAreaIds.add("aboveCenterThird");
            setOfAreaIds.add("aboveRightThird");
            setOfAreaIds.add("belowLeftThird");
            setOfAreaIds.add("belowCenterThird");
            setOfAreaIds.add("belowRightThird");
            setOfAreaIds.add("aboveLeftGoal");
            setOfAreaIds.add("slightlyAboveLeftGoal");
            setOfAreaIds.add("leftGoal");
            setOfAreaIds.add("slightlyBelowLeftGoal");
            setOfAreaIds.add("belowLeftGoal");
            setOfAreaIds.add("aboveRightGoal");
            setOfAreaIds.add("slightlyAboveRightGoal");
            setOfAreaIds.add("rightGoal");
            setOfAreaIds.add("slightlyBelowRightGoal");
            setOfAreaIds.add("belowRightGoal");
            FilterModule.ContainedInSetFilter secondEventAreaIdFilter = new FilterModule.ContainedInSetFilter(new Schema("fieldValue{areaId,true}"), setOfAreaIds);
            FilterModule secondEventAreaEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, secondEventAreaIdFilter);

            // storeKickEventModule: Stores the generation timestamp, the playerId, the teamId, the position, numPlayersNearerToGoal, attacked and zone of the last kickEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, kickTsStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, kickPlayerIdStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{groupIdentifiers,0,false}"), String.class, kickTeamIdStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, kickPlayerPosStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{numPlayersNearerToGoal,true}"), Integer.class, kickNumPlayerNearerToGoalStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{attacked,true}"), Boolean.class, kickAttackedStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{zone,true}"), String.class, kickZoneStore));
            StoreModule storeKickEventModule = new StoreModule(singleValueStoreList, null, false);

            // storeKickoffEventModule: Stores the identifier of the team which plays on the left side from the last kickoffEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList3 = new LinkedList<>();
            singleValueStoreList3.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{groupIdentifiers,1,false}"), String.class, leftTeamIdStore));
            StoreModule storeKickoffEventModule = new StoreModule(singleValueStoreList3, null, false);

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

            // passAndShotDetectionModule: Generates a successfulPassEvent for every detected successful pass, an interceptionEvent for every detected interception, a misplacedPassEvent for every detected misplaced pass, a clearanceEvent for every detected clearance, a goalEvent for every detected goal, and a shotOffTargetEvent for every detected shot off target. Moreover, generates passStatistics stream elements for every detected successful pass, interception, misplaced pass, and clearance as well as shotStatistics stream elements for every detected goal and shot off target.
            List<StatisticsItemInfo> statisticsItemInfos = new LinkedList<>();
            statisticsItemInfos.addAll(teams);
            statisticsItemInfos.addAll(players);
            PassAndShotDetectionModule passAndShotDetectionModule = new PassAndShotDetectionModule(statisticsItemInfos, maxTime, sidewardsAngleThreshold, goalHeight, kickTsStore, kickPlayerIdStore, kickTeamIdStore, kickPlayerPosStore, kickNumPlayerNearerToGoalStore, kickAttackedStore, kickZoneStore, leftTeamIdStore, currentlyBallInLeftThirdStore, currentlyBallInCenterThirdStore, currentlyBallInRightThirdStore, lastUsedKickEventTsStore, numSuccessfulPassesStore, numInterceptionsStore, numMisplacedPassesStore, numClearancesStore, numForwardPassesStore, numBackwardPassesStore, numLeftPassesStore, numRightPassesStore, sumPackingStore, numShotsOffTargetStore, numGoalsStore, hasSentInitialStatisticsStreamElementsStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeKickEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeKickEventModule, null, "storeKickEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsKickEventFilterModuleGraphElement = new LinkedList<>();
            subElementsKickEventFilterModuleGraphElement.add(storeKickEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickEventFilterModule, subElementsKickEventFilterModuleGraphElement, "kickEventFilterModule");

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

            SingleElementProcessorGraph.SingleElementProcessorGraphElement passAndShotDetectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(passAndShotDetectionModule, null, "passAndShotDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallPossessionChangeEventFilterModuleGraphElement = new LinkedList<>();
            subElementsBallPossessionChangeEventFilterModuleGraphElement.add(passAndShotDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionChangeEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionChangeEventFilterModule, subElementsBallPossessionChangeEventFilterModuleGraphElement, "ballPossessionChangeEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsSecondEventAreaEventFilterModuleGraphElement = new LinkedList<>();
            subElementsSecondEventAreaEventFilterModuleGraphElement.add(passAndShotDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement secondEventAreaEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(secondEventAreaEventFilterModule, subElementsSecondEventAreaEventFilterModuleGraphElement, "secondEventAreaEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(kickEventFilterModuleGraphElement);
            startElementsProcessGraph.add(kickoffEventFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInLeftThirdFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInCenterThirdFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInRightThirdFilterModuleGraphElement);
            startElementsProcessGraph.add(ballPossessionChangeEventFilterModuleGraphElement);
            startElementsProcessGraph.add(secondEventAreaEventFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                      +                                  +                                               +                                          +                                          +                                            +                                            +
                      | kickEvent, kickoffEvent,         | kickEvent, kickoffEvent,                      | kickEvent, kickoffEvent,                 | kickEvent, kickoffEvent,                 | kickEvent, kickoffEvent,                   | kickEvent, kickoffEvent,                   | kickEvent, kickoffEvent,                   | ballPossessionChangeEvent,       | ballPossessionChangeEvent,                    | ballPossessionChangeEvent,               | ballPossessionChangeEvent,               | ballPossessionChangeEvent,                 | ballPossessionChangeEvent,                 | ballPossessionChangeEvent,
                      | areaEvent                        | areaEvent                                     | areaEvent                                | areaEvent                                | areaEvent                                  | areaEvent                                  | areaEvent
                      |                                  |                                               |                                          |                                          |                                            |                                            |
            +---------v-----------+          +-----------v------------+                +-----------------v-------------------+       +--------------v-----------------+           +------------v--------------+               +-------------v---------------+               +------------v---------------+
            |kickEventFilterModule|          |kickoffEventFilterModule|                |ballPossessionChangeEventFilterModule|       |secondEventAreaEventFilterModule|           |ballInLeftThirdFilterModule|               |ballInCenterThirdFilterModule|               |ballInRightThirdFilterModule|
            +---------+-----------+          +-----------+------------+                +-----------------+-------------------+       +--------------+-----------------+           +------------+--------------+               +-------------+---------------+               +------------+---------------+
                      |                                  |                                               |                                          |                                          |                                            |                                            |
                      | kickEvent                        | kickoffEvent                                  | ballPossessionChangeEvent                | areaEvent                                | areaEvent                                  | areaEvent                                  | areaEvent
                      |                                  |                                               |                                          | (objId = BALL)                           | (objId = BALL)                             | (objId = BALL)                             | (objId = BALL)
                      |                                  |                                               |                                          | (areaId from set)                        | (areaId = leftThird)                       | (areaId = centerThird)                     | (areaId = rightThird)
            +---------v----------+           +-----------v-----------+                                +--v------------------------------------------v--+                          +------------v-------------+                +-------------v--------------+                +------------v--------------+
            |storeKickEventModule|           |storeKickoffEventModule|                                |              passAndShotDetectionModule        |                          |storeBallInLeftThirdModule|                |storeBallInCenterThirdModule|                |storeBallInRightThirdModule|
            +--------------------+           +-----------------------+                                +------------------------+-----------------------+                          +--------------------------+                +----------------------------+                +---------------------------+
                                                                                                                               |
                                                                                                                               | successfulPassEvent, interceptionEvent,
                                                                                                                               | misplacedPassEvent, clearanceEvent, passStatistics,
                                                                                                                               | goalEvent, shotOffTargetEvent, shotStatistics
                                                                                                                               v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
