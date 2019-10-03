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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.WindowProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.DistanceStatisticsModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.DribblingAndSpeedEventDetectionModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.StoreBallPossessionInformationModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.ActiveKeysElementProcessorModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.ActiveKeysWindowProcessorModule;
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Task that consumes fieldObjectState and generates speedLevelChangeEvents, speedLevelStatistics, dribblingEvents, dribblingStatistics, and distanceStatistics.
 * Generates a speedLevelChangeEvent when a player transits to another speed level and dribblingEvents during a dribbling actions.
 * Moreover, generates a distanceStatistics in every window() call, a speedLevelStatistics whenever the speed level changes and a dribblingStatistics whenever a dribbling action finishes.
 */
public class DistanceAndSpeedAnalysisTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(DistanceAndSpeedAnalysisTask.class);

    /**
     * Initializes DistanceAndSpeedAnalysisTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize DistanceAndSpeedAnalysisTask");
        try {
            /*======================================================
            === Read Parameters from config file                 ===
            ======================================================*/
            int activeTimeThreshold = config.getInt("streamTeam.activeTimeThreshold");
            String ballIdentifier = getString(config, "streamTeam.ball");

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

            List<String> speedLevelThresholdDefinitionList = config.getList("streamTeam.distanceAndSpeedAnalysis.speedLevelThresholds");
            List<Double> speedLevelThresholds = new LinkedList<>();
            for (String speedLevelThresholdDefinition : speedLevelThresholdDefinitionList) {
                speedLevelThresholds.add(Double.parseDouble(speedLevelThresholdDefinition));
            }

            double dribblingSpeedThreshold = config.getDouble("streamTeam.distanceAndSpeedAnalysis.dribblingSpeedThreshold");
            int dribblingTimeThreshold = config.getInt("streamTeam.distanceAndSpeedAnalysis.dribblingTimeThreshold");

            /*======================================================
            === Create Stores                                    ===
            ======================================================*/
            KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) taskContext.getStore("kvStore");

            SingleValueStore<Long> currentFieldObjectStateTsStore = new SingleValueStore<>(kvStore, "currentFieldObjectStateTs", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Geometry.Vector> currentPositionStore = new SingleValueStore<>(kvStore, "currentPosition", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<Integer> speedLevelStore = new SingleValueStore<>(kvStore, "speedLevel", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> lastSpeedLevelChangeTsStore = new SingleValueStore<>(kvStore, "lastSpeedLevelChangeTs", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long[]> speedLevelTimesStore = new SingleValueStore<>(kvStore, "speedLevelTimes", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<String> waitForDribblingStartPlayerStore = new SingleValueStore<>(kvStore, "waitForDribblingStartPlayer", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> velocityAboveDribblingSpeedThresholdStartTsStore = new SingleValueStore<>(kvStore, "velocityAboveDribblingSpeedThresholdStartTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> dribblingStartTsStore = new SingleValueStore<>(kvStore, "dribblingStartTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Geometry.Vector> dribblingLastPositionStore = new SingleValueStore<>(kvStore, "dribblingLastPosition", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Double> dribblingLengthStore = new SingleValueStore<>(kvStore, "dribblingLength", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<String> activeDribblingPlayerStore = new SingleValueStore<>(kvStore, "activeDribblingPlayer", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numDribblingsStore = new SingleValueStore<>(kvStore, "numDribblings", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> sumDribblingDurationStore = new SingleValueStore<>(kvStore, "sumDribblingDuration", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Double> sumDribblingLengthStore = new SingleValueStore<>(kvStore, "sumDribblingLength", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore = new SingleValueStore<>(kvStore, "hasSentInitialStatisticsStreamElements", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> dribblingEventIdentifierCounterStore = new SingleValueStore<>(kvStore, "dribblingEventIdentifierCounter", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore = new SingleValueStore<>(kvStore, "ballPossessionInformation", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<ArrayList<String>> activeKeysStore = new SingleValueStore<>(kvStore, "activeKeys", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> lastProcessingTimestampStore = new SingleValueStore<>(kvStore, "lastProcessingTimestamp", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> maxGenerationTimestampStore = new SingleValueStore<>(kvStore, "maxGenerationTimestampStore", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Geometry.Vector> lastUsedPositionStore = new SingleValueStore<>(kvStore, "lastUsedPosition", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Double> distanceStore = new SingleValueStore<>(kvStore, "distance", Schema.NO_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // ballPossessionChangeEventFilterModule: Forwards only ballPossessionChangeEvent stream elements
            FilterModule.EqualityFilter ballPossessionChangeEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), BallPossessionChangeEventStreamElement.STREAMNAME);
            FilterModule ballPossessionChangeEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballPossessionChangeEventStreamNameFilter);

            // storeBallPossessionModule: Stores the information if someone is in ball possession and if yes the playerId and the teamId of the last ballPossessionChangeEvent stream element
            StoreBallPossessionInformationModule storeBallPossessionModule = new StoreBallPossessionInformationModule(ballPossessionInformationStore);

            // fieldObjectStateFilterModule: Forwards only fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule fieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter);

            // playerFieldObjectStateFilterModule: Forwards only player fieldObjectState stream elements
            FilterModule.InequalityFilter playerFilter = new FilterModule.InequalityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule playerFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, playerFilter);

            // storeFieldObjectStateModule: Stores the generation timestamp and the position of the last fieldObjectState stream element of every player
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, currentFieldObjectStateTsStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, currentPositionStore));
            StoreModule storeFieldObjectStateModule = new StoreModule(singleValueStoreList, null, true);

            // dribblingAndSpeedEventDetectionModule: Generates a speedLevelChangeEvent and a speedLevelStatistics when a player transits to another speed level, dribblingEvents during a dribbling actions, and generates dribblingStatistics when a dribbling action finishes
            List<StatisticsItemInfo> statisticsItemInfos = new LinkedList<>();
            statisticsItemInfos.addAll(teams);
            statisticsItemInfos.addAll(players);
            DribblingAndSpeedEventDetectionModule dribblingAndSpeedEventDetectionModule = new DribblingAndSpeedEventDetectionModule(statisticsItemInfos, speedLevelThresholds, dribblingSpeedThreshold, dribblingTimeThreshold, speedLevelStore, lastSpeedLevelChangeTsStore, speedLevelTimesStore, ballPossessionInformationStore, waitForDribblingStartPlayerStore, velocityAboveDribblingSpeedThresholdStartTsStore, dribblingStartTsStore, dribblingLastPositionStore, dribblingLengthStore, activeDribblingPlayerStore, numDribblingsStore, sumDribblingDurationStore, sumDribblingLengthStore, hasSentInitialStatisticsStreamElementsStore, dribblingEventIdentifierCounterStore);

            // activeKeysElementProcessorModule: Updates the processing time of last data stream element received for every key and adds new keys to the activeKeys list.
            ActiveKeysElementProcessorModule activeKeysElementProcessorModule = new ActiveKeysElementProcessorModule(activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // activeKeysWindowProcessorModule: Generates an internalActiveKeys stream element for every active key when window() is triggered.
            ActiveKeysWindowProcessorModule activeKeysWindowProcessorModule = new ActiveKeysWindowProcessorModule(activeTimeThreshold, activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // distanceStatisticsModule: Generates a distanceStatistics stream element when it receives an internalActiveKeys stream element.
            DistanceStatisticsModule distanceStatisticsModule = new DistanceStatisticsModule(players, statisticsItemInfos, currentFieldObjectStateTsStore, currentPositionStore, lastUsedPositionStore, distanceStore);


            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement dribblingAndSpeedEventDetectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(dribblingAndSpeedEventDetectionModule, null, "dribblingAndSpeedEventDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStoreFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStoreFieldObjectStateModuleGraphElement.add(dribblingAndSpeedEventDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeFieldObjectStateModule, subElementsStoreFieldObjectStateModuleGraphElement, "storeFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsPlayerFieldObjectStateFilterModule = new LinkedList<>();
            subElementsPlayerFieldObjectStateFilterModule.add(storeFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement playerFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(playerFieldObjectStateFilterModule, subElementsPlayerFieldObjectStateFilterModule, "playerFieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement activeKeysElementProcessorModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(activeKeysElementProcessorModule, null, "activeKeysElementProcessorModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsFieldObjectStateFilterModuleGraphElement.add(playerFieldObjectStateFilterModuleGraphElement);
            subElementsFieldObjectStateFilterModuleGraphElement.add(activeKeysElementProcessorModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateFilterModule, subElementsFieldObjectStateFilterModuleGraphElement, "fieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallPossessionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallPossessionModule, null, "storeBallPossessionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallPossessionChangeEventFilterModuleGraphElement = new LinkedList<>();
            subElementsBallPossessionChangeEventFilterModuleGraphElement.add(storeBallPossessionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballPossessionChangeEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballPossessionChangeEventFilterModule, subElementsBallPossessionChangeEventFilterModuleGraphElement, "ballPossessionChangeEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(fieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(ballPossessionChangeEventFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            // Window Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement distanceStatisticsModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(distanceStatisticsModule, null, "distanceStatisticsModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsActiveKeysWindowProcessorModule = new LinkedList<>();
            subElementsActiveKeysWindowProcessorModule.add(distanceStatisticsModuleGraphElement);
            WindowProcessorGraph.WindowProcessorGraphStartElement activeKeysWindowProcessorModuleGraphElement = new WindowProcessorGraph.WindowProcessorGraphStartElement(activeKeysWindowProcessorModule, subElementsActiveKeysWindowProcessorModule, "activeKeysWindowProcessorModule");

            List<WindowProcessorGraph.WindowProcessorGraphStartElement> startElementsWindowGraph = new LinkedList<>();
            startElementsWindowGraph.add(activeKeysWindowProcessorModuleGraphElement);

            this.windowProcessorGraph = new WindowProcessorGraph(startElementsWindowGraph);

            /* Generated with http://asciiflow.com/
                                              +                                                            +
                                              | fieldObjectState,                                          | fieldObjectState,
                                              | ballPossessionChangeEvent                                  | ballPossessionChangeEvent
                                              |                                                            |
                                +-------------v--------------+                           +-----------------v-------------------+        +-------------------------------+
                                |fieldObjectStateFilterModule|                           |ballPossessionChangeEventFilterModule|        |activeKeysWindowProcessorModule|
                                +---+--------------------+---+                           +-----------------+-------------------+        +--------------+----------------+
                                    |                    |                                                 |                                           |
                                    | fieldObjectState   | fieldObjectState                                | ballPossessionChangeEvent                 | internalActiveKeys
                                    |                    |                                                 |                                           |
                                    |                    |                                                 |                                           |
           +------------------------v---------+       +--v-----------------------------+       +-----------v-------------+                 +-----------v------------+
           |playerFieldObjectStateFilterModule|       |activeKeysElementProcessorModule|       |storeBallPossessionModule|                 |distanceStatisticsModule|
           +------------+---------------------+       +--------------------------------+       +-------------------------+                 +-----------+------------+
                        |                                                                                                                              |
                        | fieldObjectState                                                                                                             | distanceStatistics
                        | (all but ball)                                                                                                               v
                        |
              +---------v-----------------+
              |storeFieldObjectStateModule|
              +---------+-----------------+
                        |
                        | fieldObjectState
                        | (all but ball)
                        |
          +-------------v-----------------------+
          |dribblingAndSpeedEventDetectionModule|
          +-------------+-----------------------+
                        |
                        | speedLevelChangeEvent, dribblingEvent, speedLevelStatistics, dribblingStatistics
                        v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
