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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.TeamAreaModule;
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
 * Task that consumes fieldObjectState and generates teamAreaState.
 * Generates a teamAreaState stream element whenever a fieldObjectState stream element updates an area defined by the players of a team.
 */
public class TeamAreaTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(TeamAreaTask.class);

    /**
     * Initializes TeamAreaTask.
     *
     * @param config      Config
     * @param taskContext TaskContext
     */
    @Override
    public void init(Config config, TaskContext taskContext) {
        logger.info("Initialize TeamAreaTask");
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

            SingleValueStore<Long> fieldObjectStateTsStore = new SingleValueStore<>(kvStore, "fieldObjectStateTsStore", new Schema("arrayValue{objectIdentifiers,0,false}"));
            SingleValueStore<Geometry.Vector> positionStore = new SingleValueStore<>(kvStore, "position", new Schema("arrayValue{objectIdentifiers,0,false}"));

            SingleValueStore<Double> mbrSurfaceStore = new SingleValueStore<>(kvStore, "mbrSurface", Schema.NO_INNER_KEY_SCHEMA);
            SingleValueStore<Double> pchSurfaceStore = new SingleValueStore<>(kvStore, "pchSurface", Schema.NO_INNER_KEY_SCHEMA);


            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // playerFieldObjectStateFilterModule: Forwards only player fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule.InequalityFilter playerFilter = new FilterModule.InequalityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule playerFieldObjectStateFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter, playerFilter);

            // storePlayerFieldObjectStateModule: Stores latest position for every player
            List<StoreModule.SingleValueStoreListEntry> singleValueStoreList = new LinkedList<>();
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, fieldObjectStateTsStore));
            singleValueStoreList.add(new StoreModule.SingleValueStoreListEntry(new Schema("positionValue{0}"), Geometry.Vector.class, positionStore));
            StoreModule storePlayerFieldObjectStateModule = new StoreModule(singleValueStoreList, null, true);

            // teamAreaModule: Generates a teamAreaState stream element whenever a fieldObjectState stream element updates an area defined by the players of a team
            TeamAreaModule teamAreaModule = new TeamAreaModule(players, positionStore, fieldObjectStateTsStore, mbrSurfaceStore, pchSurfaceStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement teamAreaModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(teamAreaModule, null, "teamAreaModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsStorePlayerFieldObjectStateModuleGraphElement = new LinkedList<>();
            subElementsStorePlayerFieldObjectStateModuleGraphElement.add(teamAreaModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement storePlayerFieldObjectStateModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storePlayerFieldObjectStateModule, subElementsStorePlayerFieldObjectStateModuleGraphElement, "storePlayerFieldObjectStateModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsPlayerFieldObjectStateFilterModuleGraphElement = new LinkedList<>();
            subElementsPlayerFieldObjectStateFilterModuleGraphElement.add(storePlayerFieldObjectStateModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement playerFieldObjectStateFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(playerFieldObjectStateFilterModule, subElementsPlayerFieldObjectStateFilterModuleGraphElement, "playerFieldObjectStateFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(playerFieldObjectStateFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                            +
                            | fieldObjectState,
                            |
                            |
            +---------------v------------------+
            |playerFieldObjectStateFilterModule|
            +---------------+------------------+
                            |
                            | fieldObjectState
                            | (all but ball)
                            |
            +---------------v-----------------+
            |storePlayerFieldObjectStateModule|
            +---------------+-----------------+
                            |
                            | fieldObjectState
                            | (all but ball)
                            |
                    +-------+------+
                    |teamAreaModule|
                    +-------+------+
                            |
                            | teamAreaState
                            v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
