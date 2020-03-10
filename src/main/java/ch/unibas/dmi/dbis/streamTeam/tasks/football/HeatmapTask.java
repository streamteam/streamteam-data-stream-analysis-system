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

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchMetadataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.WindowProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.HeatmapConstructionModule;
import ch.unibas.dmi.dbis.streamTeam.modules.football.HeatmapSenderModule;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Task that consumes fieldObjectState and matchMetadata and generates heatmapStatistics.
 * Generates a heatmapStatistics stream element every second for every player/team-interval-combination.
 */
public class HeatmapTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(HeatmapTask.class);

    /**
     * Creates state abstractions and module graphs for HeatmapTask.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs for HeatmapTask");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            String ballIdentifier = getString(config, "streamTeam.ball");
            int activeTimeThreshold = config.getInt("streamTeam.activeTimeThreshold");
            int numXGridCells = config.getInt("streamTeam.heatmap.grid.x");
            int numYGridCells = config.getInt("streamTeam.heatmap.grid.y");

            List<String> intervals = config.getList("streamTeam.heatmap.intervals");
            int[] timeIntervalList = new int[intervals.size()];
            for (int i = 0; i < intervals.size(); ++i) {
                timeIntervalList[i] = Integer.parseInt(intervals.get(i));
            }

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
            === Create state abstractions                        ===
            ======================================================*/
            SingleValueStore<long[][]> lastSecondHeatmapStore = new SingleValueStore<>(kvStore, "lastSecondHeatmap", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> lastPositionTsStore = new SingleValueStore<>(kvStore, "lastPositionTs", Schema.NO_INNER_KEY_SCHEMA);

            SingleValueStore<ArrayList<String>> activeKeysStore = new SingleValueStore<>(kvStore, "activeKeys", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> lastProcessingTimestampStore = new SingleValueStore<>(kvStore, "lastProcessingTimestamp", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> maxGenerationTimestampStore = new SingleValueStore<>(kvStore, "maxGenerationTimestampStore", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Double> fieldLengthStore = new SingleValueStore<>(kvStore, "fieldLength", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Double> fieldWidthStore = new SingleValueStore<>(kvStore, "fieldWidth", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<long[][]> fullGameHeatmapStore = new SingleValueStore<>(kvStore, "fullGameHeatmap", Schema.NO_INNER_KEY_SCHEMA);
            int heatmapDiffHistoryLength = 0;
            for (int timeInterval : timeIntervalList) {
                if (heatmapDiffHistoryLength < timeInterval) {
                    heatmapDiffHistoryLength = timeInterval;
                }
            }
            HistoryStore<HashMap<Integer, HashMap<Integer, Long>>> heatmapDiffHistoryStore = new HistoryStore<>(kvStore, "heatmapDiff", Schema.NO_INNER_KEY_SCHEMA, heatmapDiffHistoryLength);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // fieldObjectStateFilterModule: Forwards only fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule fieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter);

            // playerFieldObjectStateFilterModule: Forwards only player fieldObjectState stream elements
            FilterModule.InequalityFilter playerFilter = new FilterModule.InequalityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule playerFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, playerFilter);

            // heatmapConstructionModule:  Constructs/Updates the current heatmaps and the latest timestamp (for all players/teams) for every incoming fieldObjectState stream element
            HeatmapConstructionModule heatmapConstructionModule = new HeatmapConstructionModule(fieldLengthStore, fieldWidthStore, numXGridCells, numYGridCells, lastSecondHeatmapStore, lastPositionTsStore);

            // matchMetadataFilterModule: Forwards only matchMetadata stream elements
            FilterModule.EqualityFilter matchMetadataStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), MatchMetadataStreamElement.STREAMNAME);
            FilterModule matchMetadataFilterModule = new FilterModule(FilterModule.CombinationType.AND, matchMetadataStreamNameFilter);

            // matchMetadataStoreModule: Stores the fieldLength and the fieldWidth from the matchMetadata stream element
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{fieldLength,true}"), Double.class, fieldLengthStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{fieldWidth,true}"), Double.class, fieldWidthStore));
            StoreModule matchMetadataStoreModule = new StoreModule(singleValueStoreList, null, false);

            // activeKeysElementProcessorModule: Updates the processing time of last data stream element received for every key and adds new keys to the activeKeys list.
            ActiveKeysElementProcessorModule activeKeysElementProcessorModule = new ActiveKeysElementProcessorModule(activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // activeKeysWindowProcessorModule: Generates an internalActiveKeys stream element for every active key when window() is triggered.
            ActiveKeysWindowProcessorModule activeKeysWindowProcessorModule = new ActiveKeysWindowProcessorModule(activeTimeThreshold, activeKeysStore, lastProcessingTimestampStore, maxGenerationTimestampStore);

            // heatmapSenderModule: Generates a heatmapStatistics stream element for every player/team and interval when it receives an internalActiveKeys stream element.
            List<StatisticsItemInfo> statisticsItemInfos = new LinkedList<>();
            statisticsItemInfos.addAll(teams);
            statisticsItemInfos.addAll(players);
            HeatmapSenderModule heatmapSenderModule = new HeatmapSenderModule(numXGridCells, numYGridCells, statisticsItemInfos, timeIntervalList, lastSecondHeatmapStore, lastPositionTsStore, heatmapDiffHistoryStore, fullGameHeatmapStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement heatmapModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(heatmapConstructionModule, null, "heatmapConstructionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsPlayerFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsPlayerFieldObjectStateFilterModuleGraphElement.add(heatmapModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement playerFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(playerFieldObjectStateFilterModule, subElementsPlayerFieldObjectStateFilterModuleGraphElement, "playerFieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement activeKeysElementProcessorModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(activeKeysElementProcessorModule, null, "activeKeysElementProcessorModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsFieldObjectStateFilterModuleGraphElement.add(playerFieldObjectStateFilterModuleGraphElement);
            subElementsFieldObjectStateFilterModuleGraphElement.add(activeKeysElementProcessorModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateFilterModule, subElementsFieldObjectStateFilterModuleGraphElement, "fieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataStoreModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataStoreModule, null, "matchMetadataStoreModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsMatchesFilterModuleGraphElement = new LinkedList<>();
            subElementsMatchesFilterModuleGraphElement.add(matchMetadataStoreModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement matchMetadataFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(matchMetadataFilterModule, subElementsMatchesFilterModuleGraphElement, "matchMetadataFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(fieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(matchMetadataFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            // Window Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement heatmapSenderModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(heatmapSenderModule, null, "heatmapSenderModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsActiveKeysWindowProcessorModuleGraphElement = new LinkedList<>();
            subElementsActiveKeysWindowProcessorModuleGraphElement.add(heatmapSenderModuleGraphElement);
            WindowProcessorGraph.WindowProcessorGraphStartElement activeKeysModuleGraphElement = new WindowProcessorGraph.WindowProcessorGraphStartElement(activeKeysWindowProcessorModule, subElementsActiveKeysWindowProcessorModuleGraphElement, "activeKeysWindowProcessorModule");

            List<WindowProcessorGraph.WindowProcessorGraphStartElement> startElementsWindowGraph = new LinkedList<>();
            startElementsWindowGraph.add(activeKeysModuleGraphElement);

            this.windowProcessorGraph = new WindowProcessorGraph(startElementsWindowGraph);

            /* Generated with http://asciiflow.com/
                                     +                                                                 +
                                     | fieldObjectState,                                               | fieldObjectState,
                                     | matchMetadata                                                   | matchMetadata
                                     |                                                                 |
                      +--------------v----------------+                                   +------------+------------+       +-------------------------------+
                      |  fieldObjectStateFilterModule |                                   |matchMetadataFilterModule|       |activeKeysWindowProcessorModule|
                      +-+---------------------------+-+                                   +------------+------------+       +--------------+----------------+
                        |                           |                                                  |                                   |
                        | fieldObjectState          | fieldObjectState                                 | matchMetadata                     | internalActiveKeys
                        |                           |                                                  |                                   |
                        |                           |                                                  |                                   |
          +-------------v--------------------+   +--v-----------------------------+       +------------v-----------+              +--------v----------+
          |playerFieldObjectStateFilterModule|   |activeKeysElementProcessorModule|       |matchMetadataStoreModule|              |heatmapSenderModule|
          +-------------+--------------------+   +--------------------------------+       +------------------------+              +--------+----------+
                        |                                                                                                                  |
                        | fieldObjectState                                                                                                 | heatmapStatistics
                        | (all but ball)                                                                                                   v
                        |
              +---------v---------------+
              |heatmapConstructionModule|
              +-------------------------+
            */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}