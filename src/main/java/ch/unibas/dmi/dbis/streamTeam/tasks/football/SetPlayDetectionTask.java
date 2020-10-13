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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.SetPlayDetectionModule;
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
import java.util.LinkedList;
import java.util.List;

/**
 * StreamTask that specifies the analysis logic of the Set Play Detection Worker.
 * Consumes fieldObjectState, kickoffEvent, and areaEvent and generates freekickEvent, penaltyEvent, cornerkickEvent, throwinEvent, goalkickEvent, and setPlayStatistics.
 * Generates a freekickEvent stream element for every detected free kick.
 * Generates a cornerkickEvent stream element for every detected corner kick.
 * Generates a goalkickEvent stream element for every detected goal kick.
 * Generates a throwinEvent stream element for every detected throwin.
 * Generates a penaltyEvent stream element for every detected penalty.
 * Generates setPlayStatistics stream elements for every detected free kick, corner kick, goal kick, throwin, or penalty.
 */
public class SetPlayDetectionTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(SetPlayDetectionTask.class);

    /**
     * Creates state abstractions and module graphs of the Set Play Detection Worker.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs of the Set Play Detection Worker.");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            long minTimeBetweenSetPlays = config.getLong("streamTeam.setPlayDetection.minTimeBetweenSetPlays");
            double maxSetPlayDist = config.getDouble("streamTeam.setPlayDetection.maxSetPlayDist");
            double maxVabsStatic = config.getDouble("streamTeam.setPlayDetection.maxVabsStatic");
            double minVabsMovement = config.getDouble("streamTeam.setPlayDetection.minVabsMovement");
            int vabsHistoryLength = config.getInt("streamTeam.setPlayDetection.vabsHistoryLength");
            long maxTimeThrowinDetection = config.getLong("streamTeam.setPlayDetection.maxTimeThrowinDetection");

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

            String ballDefinition = getString(config, "streamTeam.ball");
            ObjectInfo ball = ObjectInfoFactoryAndModifier.createObjectInfoFromBallDefinitionString(ballDefinition);

            /*======================================================
            === Create state abstractions                        ===
            ======================================================*/
            SingleValueStore<String> leftTeamIdStore = new SingleValueStore<>(kvStore, "leftTeamId", Schema.STATIC_INNER_KEY_SCHEMA);

            SingleValueStore<Boolean> currentlyInAreaStore = new SingleValueStore<>(kvStore, "inArea", new Schema("fieldValue{areaId,true}"));
            SingleValueStore<Long> inAreaTsStore = new SingleValueStore<>(kvStore, "inAreaTs", new Schema("fieldValue{areaId,true}"));

            HistoryStore<Geometry.Vector> positionHistoryStore = new HistoryStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"), 1);
            HistoryStore<Double> velocityAbsHistoryStore = new HistoryStore<>(kvStore, "velocityAbs", new Schema("arrayValue{objectIdentifiers,0,false}"), vabsHistoryLength);

            SingleValueStore<Long> setPlayTsStore = new SingleValueStore<>(kvStore, "setPlayTs", Schema.STATIC_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numFreekicksStore = new SingleValueStore<>(kvStore, "numFreekicks", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numPenaltiesStore = new SingleValueStore<>(kvStore, "numPenalties", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numCornerkicksStore = new SingleValueStore<>(kvStore, "numCornerkicks", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numThrowinsStore = new SingleValueStore<>(kvStore, "numThrowins", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Long> numGoalkicksKicksStore = new SingleValueStore<>(kvStore, "numGoalkicks", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore = new SingleValueStore<>(kvStore, "hasSentInitialStatisticsStreamElements", Schema.STATIC_INNER_KEY_SCHEMA);

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

            // areaEventFilterModule: Forwards only areaEvent stream elements with objectIdentifiers[0] == "BALL"
            FilterModule.EqualityFilter areaEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), AreaEventStreamElement.STREAMNAME);
            FilterModule.EqualityFilter ballFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ball.getObjectId());
            FilterModule areaEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, areaEventStreamNameFilter, ballFilter);

            // storeInAreaModule: Stores inArea and the generation timestamp of the last areaEvent stream element with objectIdentifiers[0] == "BALL" (for every areaId)
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList2 = new LinkedList<>();
            singleValueStoreList2.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{inArea,true}"), Boolean.class, currentlyInAreaStore));
            singleValueStoreList2.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, inAreaTsStore));
            StoreModule storeInAreaModule = new StoreModule(singleValueStoreList2, null, false);

            // fieldObjectStateFilterModule: Forwards only fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule fieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter);

            // storeFieldObjectStateModule: Stores the position and the absolute velocity of the latest fieldObjectState stream element of every player and the ball
            List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionHistoryStore));
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{vabs,true}"), Double.class, velocityAbsHistoryStore));
            StoreModule storeFieldObjectStateModule = new StoreModule(null, historyStoreList, true);

            // ballFilterModule: Forwards only ball fieldObjectState stream elements
            FilterModule ballFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballFilter);

            // setPlayDetectionModule: Generates freekickEvents, cornerkickEvents, goalkickEvents, throwinEvents, penaltyEvents and setPlayStatistics
            List<StatisticsItemInfo> statisticsItemInfos = new LinkedList<>();
            statisticsItemInfos.addAll(teams);
            statisticsItemInfos.addAll(players);
            SetPlayDetectionModule setPlayDetectionModule = new SetPlayDetectionModule(players, ball, statisticsItemInfos, minTimeBetweenSetPlays, maxVabsStatic, minVabsMovement, vabsHistoryLength, maxSetPlayDist, maxTimeThrowinDetection, positionHistoryStore, velocityAbsHistoryStore, leftTeamIdStore, currentlyInAreaStore, inAreaTsStore, setPlayTsStore, numFreekicksStore, numPenaltiesStore, numCornerkicksStore, numThrowinsStore, numGoalkicksKicksStore, hasSentInitialStatisticsStreamElementsStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement setPlayDetectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(setPlayDetectionModule, null, "setPlayDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallFilterModuleGraphElement = new LinkedList<>();
            subElementsBallFilterModuleGraphElement.add(setPlayDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballFilterModule, subElementsBallFilterModuleGraphElement, "ballFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStoreFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStoreFieldObjectStateModuleGraphElement.add(ballFilterModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeFieldObjectStateModule, subElementsStoreFieldObjectStateModuleGraphElement, "storeFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsFieldObjectStateFilterModuleGraphElement.add(storeFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement fieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(fieldObjectStateFilterModule, subElementsFieldObjectStateFilterModuleGraphElement, "fieldObjectStateFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeKickoffEventModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeKickoffEventModule, null, "storeKickoffEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsKickoffEventFilterModule = new LinkedList<>();
            subElementsKickoffEventFilterModule.add(storeKickoffEventModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickoffEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickoffEventFilterModule, subElementsKickoffEventFilterModule, "kickoffEventFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeInAreaModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeInAreaModule, null, "storeInAreaModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsAreaEventFilterModule = new LinkedList<>();
            subElementsAreaEventFilterModule.add(storeInAreaModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement areaEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(areaEventFilterModule, subElementsAreaEventFilterModule, "areaEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(fieldObjectStateFilterModuleGraphElement);
            startElementsProcessGraph.add(kickoffEventFilterModuleGraphElement);
            startElementsProcessGraph.add(areaEventFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                             +                                          +                                     +
                             | fieldObjectState,                        | fieldObjectState,                   | fieldObjectState,
                             | kickoffEvent,                            | kickoffEvent,                       | kickoffEvent,
                             | ballInField,                             | ballInField,                        | areaEvent
                +------------+---------------+              +-----------v------------+             +----------v----------+
                |fieldObjectStateFilterModule|              |kickoffEventFilterModule|             |areaEventFilterModule|
                +------------+---------------+              +-----------+------------+             +----------+----------+
                             |                                          |                                     |
                             | fieldObjectState                         | kickoffEvent                        | areaEvent
                             |                                          |                                     | (objId = BALL)
                             |                                          |                                     |
                +------------+--------------+               +-----------v-----------+               +---------v-------+
                |storeFieldObjectStateModule|               |storeKickoffEventModule|               |storeInAreaModule|
                +------------+--------------+               +-----------------------+               +-----------------+
                             |
                             | fieldObjectState
                             |
                             |
                     +-------+--------+
                     |ballFilterModule|
                     +-------+--------+
                             |
                             | fieldObjectState
                             | (only ball)
                             |
                   +---------+------------+
                   |setPlayDetectionModule|
                   +---------+------------+
                             |
                             | freekickEvent, cornerkickEvent, goalkickEvent, throwinEvent, penaltyEvent, setPlayStatistics
                             v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
