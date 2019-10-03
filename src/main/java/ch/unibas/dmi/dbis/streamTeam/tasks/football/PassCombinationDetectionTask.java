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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.PassCombinationDetectionModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.*;
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
 * Task that consumes successfulPassEvent, misplacedPassEvent, clearanceEvent, and interceptionEvent and that generates passSequenceEvent, doublePassEvent, and passSequenceStatistics.
 * Generates a passSequenceEvent when there is an uninterrupted sequence of successful passes (min 2) and a doublePassEvent when there is an uninterrupted double pass.
 * Each doublePassEvent is also a passSequenceEvent.
 * Moreover, generates passSequenceStatistics when a pass sequence is detected.
 */
public class PassCombinationDetectionTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PassCombinationDetectionTask.class);


    /**
     * Initializes PassCombinationDetectionTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize PassCombinationDetectionTask");
        try {
            /*======================================================
            === Read Parameters from config file                 ===
            ======================================================*/
            int successfulPassEventHistoryLength = config.getInt("streamTeam.passCombinationDetection.successfulPassEventHistoryLength");
            long maxTimeBetweenPasses = config.getInt("streamTeam.passCombinationDetection.maxTimeBetweenPasses");
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

            /*======================================================
            === Create Stores                                    ===
            ======================================================*/
            KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) taskContext.getStore("kvStore");

            HistoryStore<Long> successfulPassTsHistoryStore = new HistoryStore<>(kvStore, "successfulPassTs", Schema.STATIC_INNER_KEY_SCHEMA, successfulPassEventHistoryLength);
            HistoryStore<String> successfulPassTeamIdHistoryStore = new HistoryStore<>(kvStore, "successfulPassTeamId", Schema.STATIC_INNER_KEY_SCHEMA, successfulPassEventHistoryLength);
            HistoryStore<String> successfulPassKickPlayerIdHistoryStore = new HistoryStore<>(kvStore, "successfulPassKickPlayerId", Schema.STATIC_INNER_KEY_SCHEMA, successfulPassEventHistoryLength);
            HistoryStore<Geometry.Vector> successfulPassKickPosHistoryStore = new HistoryStore<>(kvStore, "successfulPassKickPos", Schema.STATIC_INNER_KEY_SCHEMA, successfulPassEventHistoryLength);
            HistoryStore<String> successfulPassReceivePlayerIdHistoryStore = new HistoryStore<>(kvStore, "successfulPassReceivePlayerId", Schema.STATIC_INNER_KEY_SCHEMA, successfulPassEventHistoryLength);
            HistoryStore<Geometry.Vector> successfulPassReceivePosHistoryStore = new HistoryStore<>(kvStore, "successfulPassReceivePos", Schema.STATIC_INNER_KEY_SCHEMA, successfulPassEventHistoryLength);

            SingleValueStore<Long> interceptionTsStore = new SingleValueStore<>(kvStore, "interceptionTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> misplacedPassTsStore = new SingleValueStore<>(kvStore, "misplacedPassTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> clearanceTsStore = new SingleValueStore<>(kvStore, "clearanceTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> ballLeftFieldTsStore = new SingleValueStore<>(kvStore, "ballLeftFieldTs", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Long> firstTsOfLastPassSequenceStore = new SingleValueStore<>(kvStore, "firstTsOfLastPassSequence", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numPassSequencesStore = new SingleValueStore<>(kvStore, "numPassSequences", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> maxPassSequenceLengthStore = new SingleValueStore<>(kvStore, "maxPassSequenceLength", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> sumPassSequenceLengthStore = new SingleValueStore<>(kvStore, "sumPassSequenceLength", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numDoublePassesStore = new SingleValueStore<>(kvStore, "numDoublePasses", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore = new SingleValueStore<>(kvStore, "hasSentInitialStatisticsStreamElements", Schema.NO_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // successfulPassEventFilterModule: Forwards only successfulPassEvent stream elements
            FilterModule.EqualityFilter successfulPassEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), SuccessfulPassEventStreamElement.STREAMNAME);
            FilterModule successfulPassEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, successfulPassEventStreamNameFilter);

            // storeSuccessfulPassEventModule: Stores the generation timestamp, the teamId, the kickPlayerId, the kickPlayerPos, the receivePlayerId, and the receivePlayerPos for every successfulPassEvent stream element (up to the history length)
            List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, successfulPassTsHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, successfulPassKickPosHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{1}"), Geometry.Vector.class, successfulPassReceivePosHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("arrayValue{objectIdentifiers,0,false}"), String.class, successfulPassKickPlayerIdHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("arrayValue{objectIdentifiers,1,false}"), String.class, successfulPassReceivePlayerIdHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("arrayValue{groupIdentifiers,0,false}"), String.class, successfulPassTeamIdHistoryStore));
            StoreModule storeSuccessfulPassEventModule = new StoreModule(null, historyStoreList, true);

            // passCombinationDetectionModule: Generates a passSequenceEvent and passSequenceStatistics for every detected pass sequence and a doublePassEvent for every detected double pass
            List<StatisticsItemInfo> statisticsItemInfos = new LinkedList<>();
            statisticsItemInfos.addAll(teams);
            statisticsItemInfos.addAll(players);
            PassCombinationDetectionModule passCombinationDetectionModule = new PassCombinationDetectionModule(statisticsItemInfos, maxTimeBetweenPasses, successfulPassTsHistoryStore, successfulPassTeamIdHistoryStore, successfulPassKickPlayerIdHistoryStore, successfulPassReceivePlayerIdHistoryStore, successfulPassKickPosHistoryStore, successfulPassReceivePosHistoryStore, interceptionTsStore, misplacedPassTsStore, clearanceTsStore, ballLeftFieldTsStore, firstTsOfLastPassSequenceStore, numPassSequencesStore, maxPassSequenceLengthStore, sumPassSequenceLengthStore, numDoublePassesStore, hasSentInitialStatisticsStreamElementsStore);

            // interceptionEventFilterModule: Forwards only interceptionEvent stream elements
            FilterModule.EqualityFilter interceptionEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), InterceptionEventStreamElement.STREAMNAME);
            FilterModule interceptionEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, interceptionEventStreamNameFilter);

            // storeInterceptionEventModule: Stores the generation timestamp for the last interceptionEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, interceptionTsStore));
            StoreModule storeInterceptionEventModule = new StoreModule(singleValueStoreList, null, false);

            // misplacedPassEventFilterModule: Forwards only misplacedPassEvent stream elements
            FilterModule.EqualityFilter misplacedPassEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), MisplacedPassEventStreamElement.STREAMNAME);
            FilterModule misplacedPassEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, misplacedPassEventStreamNameFilter);

            // storeMisplacedPassEventModule: Stores the generation timestamp for the last misplacedPassEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList2 = new LinkedList<>();
            singleValueStoreList2.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, misplacedPassTsStore));
            StoreModule storeMisplacedPassEventModule = new StoreModule(singleValueStoreList2, null, false);

            // clearanceEventFilterModule: Forwards only celaranceEvent stream elements
            FilterModule.EqualityFilter clearanceEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), ClearanceEventStreamElement.STREAMNAME);
            FilterModule clearanceEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, clearanceEventStreamNameFilter);

            // storeClearanceEventModule: Stores the generation timestamp for the last clearanceEvent stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList3 = new LinkedList<>();
            singleValueStoreList3.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, clearanceTsStore));
            StoreModule storeClearanceEventModule = new StoreModule(singleValueStoreList3, null, false);

            // ballLeftFieldFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL", payload.areaId == "field", and payload.inArea = false
            FilterModule.EqualityFilter areaEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), AreaEventStreamElement.STREAMNAME);
            FilterModule.EqualityFilter ballObjectFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule.EqualityFilter fieldAreaFilter = new FilterModule.EqualityFilter(new Schema("fieldValue{areaId,true}"), "field");
            FilterModule.EqualityFilter inAreaFilter = new FilterModule.EqualityFilter(new Schema("fieldValue{inArea,true}"), false);
            FilterModule ballLeftFieldFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballObjectFilter, fieldAreaFilter, inAreaFilter);

            // storeBallLeftFieldModule: Stores the generation timestamp for the last areaEvent stream elements with objectIdentifiers[0] == "BALL", payload.areaId == "field", and payload.inArea = false
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList4 = new LinkedList<>();
            singleValueStoreList4.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, ballLeftFieldTsStore));
            StoreModule storeBallLeftFieldModule = new StoreModule(singleValueStoreList4, null, false);


            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement passCombinationDetectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(passCombinationDetectionModule, null, "passCombinationDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStoreSuccessfulPassEventModuleGraphElement = new LinkedList<>();
            subElementsStoreSuccessfulPassEventModuleGraphElement.add(passCombinationDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeSuccessfulPassEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeSuccessfulPassEventModule, subElementsStoreSuccessfulPassEventModuleGraphElement, "storeSuccessfulPassEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsSuccessfulPassEventFilterModuleGraphElement = new LinkedList<>();
            subElementsSuccessfulPassEventFilterModuleGraphElement.add(storeSuccessfulPassEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement successfulPassEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(successfulPassEventFilterModule, subElementsSuccessfulPassEventFilterModuleGraphElement, "successfulPassEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeInterceptionEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeInterceptionEventModule, null, "storeInterceptionEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsInterceptionEventStreamNameFilterModuleGrapheElement = new LinkedList<>();
            subElementsInterceptionEventStreamNameFilterModuleGrapheElement.add(storeInterceptionEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement interceptionEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(interceptionEventFilterModule, subElementsInterceptionEventStreamNameFilterModuleGrapheElement, "interceptionEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeMisplacedPassEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeMisplacedPassEventModule, null, "storeMisplacedPassEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsMisplacedPassEventStreamNameFilterModuleGraphElement = new LinkedList<>();
            subElementsMisplacedPassEventStreamNameFilterModuleGraphElement.add(storeMisplacedPassEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement misplacedPassEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(misplacedPassEventFilterModule, subElementsMisplacedPassEventStreamNameFilterModuleGraphElement, "misplacedPassEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeClearanceEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeClearanceEventModule, null, "storeClearanceEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsClearanceEventStreamNameFilterModuleGraphElement = new LinkedList<>();
            subElementsClearanceEventStreamNameFilterModuleGraphElement.add(storeClearanceEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement clearanceEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(clearanceEventFilterModule, subElementsClearanceEventStreamNameFilterModuleGraphElement, "clearanceEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeBallLeftFieldModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeBallLeftFieldModule, null, "storeBallLeftFieldModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallLeftFieldFilterModule = new LinkedList<>();
            subElementsBallLeftFieldFilterModule.add(storeBallLeftFieldModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballLeftFieldFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballLeftFieldFilterModule, subElementsBallLeftFieldFilterModule, "ballLeftFieldFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(successfulPassEventFilterModuleGraphElement);
            startElementsProcessGraph.add(interceptionEventFilterModuleGraphElement);
            startElementsProcessGraph.add(misplacedPassEventFilterModuleGraphElement);
            startElementsProcessGraph.add(clearanceEventFilterModuleGraphElement);
            startElementsProcessGraph.add(ballLeftFieldFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);


            /* Generated with http://asciiflow.com/
                           +                                      +                                        +                                       +                                     +
                           | successfulPassEvent,                 | successfulPassEvent,                   | successfulPassEvent,                  | successfulPassEvent,                | successfulPassEvent,
                           | interceptionEvent,                   | interceptionEvent,                     | interceptionEvent,                    | interceptionEvent,                  | interceptionEvent,
                           | misplacedPassEvent,                  | misplacedPassEvent,                    | misplacedPassEvent,                   | misplacedPassEvent,                 | misplacedPassEvent,
                           | clearanceEvent,                      | clearanceEvent,                        | clearanceEvent,                       | clearanceEvent,                     | clearanceEvent,
                           | areaEvent                            | areaEvent                              | areaEvent                             | areaEvent                           | areaEvent
            +--------------v----------------+       +-------------v---------------+          +-------------v----------------+          +-----------v--------------+          +-----------v-------------+
            |successfulPassEventFilterModule|       |interceptionEventFilterModule|          |misplacedPassEventFilterModule|          |clearanceEventFilterModule|          |ballLeftFieldFilterModule|
            +--------------+----------------+       +-------------+---------------+          +-------------+----------------+          +-----------+--------------+          +-----------+-------------+
                           |                                      |                                        |                                       |                                     |
                           | successfulPassEvent                  | interceptionEvent                      | misplacedPassEvent                    | clearanceEvent                      | areaEvent
                           |                                      |                                        |                                       |                                     | (objId = BALL)
                           |                                      |                                        |                                       |                                     | (areaId = field)
                           |                                      |                                        |                                       |                                     | (inArea = false)
            +--------------v---------------+        +-------------v--------------+           +-------------v---------------+           +-----------v-------------+           +-----------v------------+
            |storeSuccessfulPassEventModule|        |storeInterceptionEventModule|           |storeMisplacedPassEventModule|           |storeClearanceEventModule|           |storeBallLeftFieldModule|
            +--------------+---------------+        +----------------------------+           +-----------------------------+           +-------------------------+           +------------------------+
                           |
                           | successfulPassEvent
                           |
                           |
            +--------------v---------------+
            |passCombinationDetectionModule|
            +--------------+---------------+
                           |
                           | passSequenceEvent, doublePasEvent, passSequenceStatistics
                           v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
