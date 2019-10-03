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

import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.KickoffDetectionModule;
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
import java.util.LinkedList;
import java.util.List;

/**
 * Task that consumes fieldObjectState and generates kickoffEvent.
 * Generates a kickoffEvent stream element for every detected kickoff.
 */
public class KickoffDetectionTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(KickoffDetectionTask.class);

    /**
     * Initializes KickoffDetectionTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize KickoffDetectionTask");
        try {
            /*======================================================
            === Read Parameters from config file                 ===
            ======================================================*/
            double maxPlayerMidpointDist = config.getDouble("streamTeam.kickoffDetection.maxPlayerMidpointDist");
            double maxBallMidpointDist = config.getDouble("streamTeam.kickoffDetection.maxBallMidpointDist");
            long minTimeBetweenKickoffs = config.getLong("streamTeam.kickoffDetection.minTimeBetweenKickoffs");
            double minPlayerMidlineDist = config.getDouble("streamTeam.kickoffDetection.minPlayerMidlineDist");

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
            === Create Stores                                    ===
            ======================================================*/
            KeyValueStore<String, Serializable> kvStore = (KeyValueStore<String, Serializable>) taskContext.getStore("kvStore");

            SingleValueStore<Geometry.Vector> positionStore = new SingleValueStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<Long> kickoffTsStore = new SingleValueStore<Long>(kvStore, "kickoffTs", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // storeFieldObjectStateModule: Stores the latest position for every player and the ball
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
            StoreModule storeFieldObjectStateModule = new StoreModule(singleValueStoreList, null, true);

            // ballFieldObjectStateFilterModule: Forwards only ball fieldObjectState stream elements
            FilterModule.EqualityFilter ballFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ball.getObjectId());
            FilterModule ballFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, ballFilter);

            // kickoffDetectionModule: Detects kickoffs and generates kickoffEvent stream elements
            KickoffDetectionModule kickoffDetectionModule = new KickoffDetectionModule(players, teams, ball, maxPlayerMidpointDist, maxBallMidpointDist, minTimeBetweenKickoffs, minPlayerMidlineDist, positionStore, kickoffTsStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickoffDetectionModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickoffDetectionModule, null, "kickoffDetectionModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsBallFieldObjectStateFilterModuleGraphElement.add(kickoffDetectionModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballFieldObjectStateFilterModule, subElementsBallFieldObjectStateFilterModuleGraphElement, "ballFieldObjectStateFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStoreFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStoreFieldObjectStateModuleGraphElement.add(ballFieldObjectStateFilterModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeFieldObjectStateModule, subElementsStoreFieldObjectStateModuleGraphElement, "storeFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(storeFieldObjectStateModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                           +
                           | fieldObjectState
                           |
                           |
              +------------v--------------+
              |storeFieldObjectStateModule|
              +------------+--------------+
                           |
                           | fieldObjectState
                           |
                           |
            +--------------v-----------------+
            |ballFieldObjectStateFilterModule|
            +--------------+-----------------+
                           |
                           | fieldObjectState
                           | (only ball)
                           |
                +----------v-----------+
                |kickoffDetectionModule|
                +----------+-----------+
                           | kickoffEvent
                           |
                           v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException | KickoffDetectionModule.KickoffDetectionModuleInitializationException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
