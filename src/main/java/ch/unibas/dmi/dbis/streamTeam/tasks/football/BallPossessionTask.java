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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickoffEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchMetadataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.WindowProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.BallPossessionChangeDetectionModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.BallPossessionStatisticsModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.ActiveKeysElementProcessorModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.ActiveKeysWindowProcessorModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.*;
import ch.unibas.dmi.dbis.streamTeam.tasks.AbstractTask;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * StreamTask that specifies the analysis logic of the Ball Possession Worker.
 * Consumes fieldObjectState, matchMetadata, kickoffEvent, and areaEvent and generates ballPossessionChangeEvent, duelEvent, and ballPossessionStatistics.
 * Generates a ballPossessionChangeEvent stream element for every change in ball possession and duelEvent stream elements during a duel action.
 * Moreover, periodically generates ballPossessionStatistics stream elements.
 */
public class BallPossessionTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(BallPossessionTask.class);

    /**
     * Creates state abstractions and module graphs of the  Ball Possession Worker.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs of the Ball Possession Worker.");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            int activeTimeThreshold = config.getInt("streamTeam.activeTimeThreshold");
            double minVabsDiff = config.getDouble("streamTeam.ballPossession.minVabsDiff");
            double maxVabsForVabsDiff = config.getDouble("streamTeam.ballPossession.maxVabsForVabsDiff");
            double minMovingDirAngleDiff = config.getDouble("streamTeam.ballPossession.minMovingDirAngleDiff");
            double maxBallPossessionChangeDist = config.getDouble("streamTeam.ballPossession.maxBallPossessionChangeDist");
            double maxDuelDist = config.getDouble("streamTeam.ballPossession.maxDuelDist");

            List<String> playersDefinitionList = config.getList("streamTeam.players");
            List<ObjectInfo> players = new LinkedList<>();
            for (String playerDefinition : playersDefinitionList) {
                players.add(ObjectInfoFactoryAndModifier.createObjectInfoFromPlayerDefinitionString(playerDefinition));
            }

            List<String> teamsDefinitionList = config.getList("streamTeam.teams");
            if (teamsDefinitionList.size() != 2) {
                throw new ConfigException("No support for " + teamsDefinitionList.size() + " teams. Only supports 2 teams.");
            }
            List<GroupInfo> teams = new LinkedList<>();
            for (String teamDefinition : teamsDefinitionList) {
                teams.add(GroupInfoFactory.createGroupInfoFromTeamDefinitionString(teamDefinition));
            }

            String ballDefinition = getString(config, "streamTeam.ball");
            ObjectInfo ball = ObjectInfoFactoryAndModifier.createObjectInfoFromBallDefinitionString(ballDefinition);

            /*======================================================
            === Create state abstractions                        ===
            ======================================================*/
            SingleValueStore<String> leftTeamIdStore = new SingleValueStore<>(kvStore, "leftTeamId", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> currentlyBallInFieldStore = new SingleValueStore<>(kvStore, "currentlyBallInField", Schema.STATIC_INNER_KEY_SCHEMA);

            HistoryStore<Long> fieldObjectStateTsHistoryStore = new HistoryStore<>(kvStore, "fieldObjectStateTs", new Schema("arrayValue{objectIdentifiers,0,false}"), 2);
            HistoryStore<Geometry.Vector> positionHistoryStore = new HistoryStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"), 3);
            HistoryStore<Double> velocityAbsHistoryStore = new HistoryStore<>(kvStore, "velocityAbs", new Schema("arrayValue{objectIdentifiers,0,false}"), 2);

            SingleValueStore<Double> fieldLengthStore = new SingleValueStore<>(kvStore, "fieldLength", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<String> playerInBallPossessionStore = new SingleValueStore<>(kvStore, "playerInBallPossession", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> teamInBallPossessionStore = new SingleValueStore<>(kvStore, "teamInBallPossession", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> defendingPlayerStore = new SingleValueStore<>(kvStore, "defendingPlayer", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> attackingPlayerStore = new SingleValueStore<>(kvStore, "attackingPlayer", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> duelEventIdentifierCounterStore = new SingleValueStore<>(kvStore, "duelEventIdentifierCounter", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<ArrayList<String>> activeKeysStore = new SingleValueStore<>(kvStore, "activeKeys", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> lastProcessingTimestampStore = new SingleValueStore<>(kvStore, "lastProcessingTimestamp", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> maxGenerationTimestampStore = new SingleValueStore<>(kvStore, "maxGenerationTimestampStore", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Long> ballPossessionTimeStore = new SingleValueStore<>(kvStore, "ballPossessionTime", Schema.NO_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // kickoffEventFilterModule: Forwards only kickoffEvent stream elements
            FilterModule.EqualityFilter kickoffEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), KickoffEventStreamElement.STREAMNAME);
            FilterModule kickoffEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, kickoffEventStreamNameFilter);

            // storeKickoffEventModule: Stores the identifier of the team which plays on the left side from the last kickoffEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("arrayValue{groupIdentifiers,1,false}"), String.class, leftTeamIdStore));
            StoreModule storeKickoffEventModule = new StoreModule(singleValueStoreList, null, false);

            // ballInFieldFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL" and payload.areaId == "field"
            FilterModule.EqualityFilter areaEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), AreaEventStreamElement.STREAMNAME);
            FilterModule.EqualityFilter ballObjectFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ball.getObjectId());
            FilterModule.EqualityFilter fieldAreaFilter = new FilterModule.EqualityFilter(new Schema("fieldValue{areaId,true}"), "field");
            FilterModule ballInFieldFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, fieldAreaFilter);

            // storeBallInFieldModule: Stores inArea of the last areaEvent stream element with objectIdentifiers[0] == "BALL" and payload.areaId == "field"
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList2 = new LinkedList<>();
            singleValueStoreList2.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{inArea,true}"), Boolean.class, currentlyBallInFieldStore));
            StoreModule storeBallInFieldModule = new StoreModule(singleValueStoreList2, null, false);

            // matchMetadataFilterModule: Forwards only matchMetadata stream elements
            FilterModule.EqualityFilter matchMetadataStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), MatchMetadataStreamElement.STREAMNAME);
            FilterModule matchMetadataFilterModule = new FilterModule(FilterModule.CombinationType.AND, matchMetadataStreamNameFilter);

            // matchMetadataStoreModule: Stores the fieldLength from the matchMetadata stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList3 = new LinkedList<>();
            singleValueStoreList3.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{fieldLength,true}"), Double.class, fieldLengthStore));
            StoreModule matchMetadataStoreModule = new StoreModule(singleValueStoreList3, null, false);

            // fieldObjectStateFilterModule: Forwards only fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule fieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter);

            // storeFieldObjectStateModule: Stores the generation timestamp, the position, and the absolute velocity of the last fieldObjectState stream element of every player and the ball
            List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, fieldObjectStateTsHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{vabs,true}"), Double.class, velocityAbsHistoryStore));
            StoreModule storeFieldObjectStateModule = new StoreModule(null, historyStoreList, true);

            // ballFilterModule: Forwards only ball fieldObjectState stream elements
            FilterModule.EqualityFilter ballFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ball.getObjectId());
            FilterModule ballFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballFilter);

            // ballPossessionChangeDetectionModule: Detects changes in ball possession as well as duels and generates ballPossessionChangeEvent and duelEvent stream elements
            BallPossessionChangeDetectionModule ballPossessionChangeDetectionModule = new BallPossessionChangeDetectionModule(players, ball, minVabsDiff, maxVabsForVabsDiff, minMovingDirAngleDiff, maxBallPossessionChangeDist, maxDuelDist, fieldLengthStore, positionHistoryStore, velocityAbsHistoryStore, leftTeamIdStore, currentlyBallInFieldStore, playerInBallPossessionStore, teamInBallPossessionStore, defendingPlayerStore, attackingPlayerStore, duelEventIdentifierCounterStore);

            // activeKeysElementProcessorModule: Updates the processing time of last data stream element received for every key and adds new keys to the activeKeys list.
            ActiveKeysElementProcessorModule activeKeysElementProcessorModule = new ActiveKeysElementProcessorModule(activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // activeKeysWindowProcessorModule: Generates an internalActiveKeys stream element for every active key when window() is triggered.
            ActiveKeysWindowProcessorModule activeKeysWindowProcessorModule = new ActiveKeysWindowProcessorModule(activeTimeThreshold, activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // ballPossessionStatisticsModule: Generates a ballPossessionStatistics stream element when it receives an internalActiveKeys stream element.
            List<StatisticsItemInfo> statisticsItemInfos = new LinkedList<>();
            statisticsItemInfos.addAll(teams);
            statisticsItemInfos.addAll(players);
            BallPossessionStatisticsModule ballPossessionStatisticsModule = new BallPossessionStatisticsModule(statisticsItemInfos, ball, fieldObjectStateTsHistoryStore, playerInBallPossessionStore, teamInBallPossessionStore, ballPossessionTimeStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionChangeDectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionChangeDetectionModule, null, "ballPossessionChangeDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallFilterModuleGraphElement = new LinkedList<>();
            subElementsBallFilterModuleGraphElement.add(ballPossessionChangeDectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballFilterModule, subElementsBallFilterModuleGraphElement, "ballFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStoreFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStoreFieldObjectStateModuleGraphElement.add(ballFilterModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeFieldObjectStateModule, subElementsStoreFieldObjectStateModuleGraphElement, "storeFieldObjectStateModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement activeKeysElementProcessorModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(activeKeysElementProcessorModule, null, "activeKeysElementProcessorModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsFieldObjectStateFilterModuleGraphElement.add(storeFieldObjectStateModuleGraphElement);
            subElementsFieldObjectStateFilterModuleGraphElement.add(activeKeysElementProcessorModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateFilterModule, subElementsFieldObjectStateFilterModuleGraphElement, "fieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeKickoffEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeKickoffEventModule, null, "storeKickoffEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsKickoffEventFilterModuleGraphElement = new LinkedList<>();
            subElementsKickoffEventFilterModuleGraphElement.add(storeKickoffEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickoffEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickoffEventFilterModule, subElementsKickoffEventFilterModuleGraphElement, "kickoffEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallInFieldModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallInFieldModule, null, "storeBallInFieldModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallInFieldFilterModuleGraphElement = new LinkedList<>();
            subElementsBallInFieldFilterModuleGraphElement.add(storeBallInFieldModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballInFieldFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballInFieldFilterModule, subElementsBallInFieldFilterModuleGraphElement, "ballInFieldFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataStoreModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataStoreModule, null, "matchMetadataStoreModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsMatchesFilterModuleGraphElement = new LinkedList<>();
            subElementsMatchesFilterModuleGraphElement.add(matchMetadataStoreModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataFilterModule, subElementsMatchesFilterModuleGraphElement, "matchMetadataFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(fieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(kickoffEventFilterModuleGraphElement);
            startElementsProcessGraph.add(ballInFieldFilterModuleGraphElement);
            startElementsProcessGraph.add(matchMetadataFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            // Window Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionStatisticsModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionStatisticsModule, null, "ballPossessionStatisticsModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsActiveKeysWindowProcessorModule = new LinkedList<>();
            subElementsActiveKeysWindowProcessorModule.add(ballPossessionStatisticsModuleGraphElement);
            WindowProcessorGraph.WindowProcessorGraphStartElement activeKeysModuleGraphElement = new WindowProcessorGraph.WindowProcessorGraphStartElement(activeKeysWindowProcessorModule, subElementsActiveKeysWindowProcessorModule, "activeKeysWindowProcessorModule");

            List<WindowProcessorGraph.WindowProcessorGraphStartElement> startElementsWindowGraph = new LinkedList<>();
            startElementsWindowGraph.add(activeKeysModuleGraphElement);

            this.windowProcessorGraph = new WindowProcessorGraph(startElementsWindowGraph);

            /* Generated with http://asciiflow.com/
                                     +                                                           +                                +                                   +
                                     | fieldObjectState,                                         | fieldObjectState,              | fieldObjectState,                 | fieldObjectState,
                                     | matchMetadata,                                            | matchMetadata,                 | matchMetadata,                    | matchMetadata,
                                     | kickoffEvent,                                             | kickoffEvent,                  | kickoffEvent                      | kickoffEvent,
                                     | areaEvent                                                 | areaEvent                      | areaEvent                         | areaEvent
                        +------------v---------------+                               +-----------v------------+      +------------+------------+          +-----------v-----------+       +-------------------------------+
                        |fieldObjectStateFilterModule|                               |kickoffEventFilterModule|      |matchMetadataFilterModule|          |ballInFieldFilterModule|       |activeKeysWindowProcessorModule|
                        +-+------------------------+-+                               +-----------+------------+      +------------+------------+          +-----------+-----------+       +--------------+----------------+
                          |                        |                                             |                                |                                   |                                  |
                          | fieldObjectState       | fieldObjectState                            | kickoffEvent                   | matchMetadata                     | areaEvent                        | internalActiveKeys
                          |                        |                                             |                                |                                   | (objId = BALL)                   |
                          |                        |                                             |                                |                                   | (areaId = field)                 |
             +------------v--------------+     +---v----------------------------+    +-----------v-----------+       +------------v-----------+           +-----------v----------+         +-------------v----------------+
             |storeFieldObjectStateModule|     |activeKeysElementProcessorModule|    |storeKickoffEventModule|       |matchMetadataStoreModule|           |storeBallInFieldModule|         |ballPossessionStatisticsModule|
             +------------+--------------+     +--------------------------------+    +-----------------------+       +------------------------+           +----------------------+         +-------------+----------------+
                          |                                                                                                                                                                              |
                          | fieldObjectState                                                                                                                                                             | ballPossessionStatistics
                          |                                                                                                                                                                              v
                          |
                  +-------v--------+
                  |ballFilterModule|
                  +-------+--------+
                          |
                          | fieldObjectState
                          | (only ball)
                          |
         +----------------v------------------+
         |ballPossessionChangeDetectionModule|
         +----------------+------------------+
                          |
                          | ballPossessionChangeEvent,
                          | duelEvent
                          v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
