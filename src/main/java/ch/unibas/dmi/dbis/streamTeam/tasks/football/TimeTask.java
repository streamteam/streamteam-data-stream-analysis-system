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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickoffEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorGraph;
import ch.unibas.dmi.dbis.streamTeam.modules.football.TimeModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.FilterModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.StoreModule;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
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
 * StreamTask that specifies the analysis logic of the Time Worker.
 * Consumes fieldObjectState and kickoffEvent and generates matchTimeProgressEvent.
 * Generates a matchTimeProgressEvent stream element every second after the first kickoff.
 */
public class TimeTask extends AbstractTask {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(TimeTask.class);

    /**
     * Creates state abstractions and module graphs of the Time Worker.
     *
     * @param config  Config
     * @param kvStore Samza key-value store for storing the state
     */
    @Override
    public void createStateAbstractionsAndModuleGraphs(Config config, KeyValueStore<String, Serializable> kvStore) {
        logger.info("Creating state abstractions and module graphs of the Time Worker.");
        try {
            /*======================================================
            === Read parameters from config file                 ===
            ======================================================*/
            String ballIdentifier = getString(config, "streamTeam.ball");
            int kickoffEventTsHistoryLength = config.getInt("streamTeam.time.kickoffEventTsHistoryLength");

            /*======================================================
            === Create state abstractions                        ===
            ======================================================*/
            HistoryStore<Long> kickoffEventTsHistoryStore = new HistoryStore<>(kvStore, "kickoffEventTs", Schema.STATIC_INNER_KEY_SCHEMA, kickoffEventTsHistoryLength);

            SingleValueStore<Long> lastTimeInSStore = new SingleValueStore<>(kvStore, "lastTimeInS", Schema.STATIC_INNER_KEY_SCHEMA);

            /*======================================================
            === Create modules                                   ===
            ======================================================*/

            // kickoffEventFilterModule: Forwards only kickoffEvent stream elements
            FilterModule.EqualityFilter kickoffEventStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), KickoffEventStreamElement.STREAMNAME);
            FilterModule kickoffEventFilterModule = new FilterModule(FilterModule.CombinationType.AND, kickoffEventStreamNameFilter);

            // storeKickoffEventModule: Stores the generation timestamp of all (last kickoffEventTsHistoryLength) kickoffEvent stream elements
            List<StoreModule.HistoryStoreListEntry> historyStoreList = new LinkedList<>();
            historyStoreList.add(new StoreModule.HistoryStoreListEntry(new Schema("fieldValue{generationTimestamp,false}"), Long.class, kickoffEventTsHistoryStore));
            StoreModule storeKickoffEventModule = new StoreModule(null, historyStoreList, false);

            // ballFilterModule: Forwards only ball fieldObjectState stream elements
            FilterModule.EqualityFilter fieldObjectStateStreamNameFilter = new FilterModule.EqualityFilter(new Schema("streamName"), FieldObjectStateStreamElement.STREAMNAME);
            FilterModule.EqualityFilter ballFilter = new FilterModule.EqualityFilter(new Schema("arrayValue{objectIdentifiers,0,false}"), ballIdentifier);
            FilterModule ballFilterModule = new FilterModule(FilterModule.CombinationType.AND, fieldObjectStateStreamNameFilter, ballFilter);

            // timeModule: Generates matchTimeProgressEvent stream elements
            TimeModule timeModule = new TimeModule(kickoffEventTsHistoryStore, lastTimeInSStore);

            /*======================================================
            === Create module graphs                             ===
            ======================================================*/

            // Single Element Processor Graph construction
            SingleElementProcessorGraph.SingleElementProcessorGraphElement timeModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(timeModule, null, "timeModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsBallFilterModuleGraphElement = new LinkedList<>();
            subElementsBallFilterModuleGraphElement.add(timeModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement ballFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(ballFilterModule, subElementsBallFilterModuleGraphElement, "ballFilterModule");

            SingleElementProcessorGraph.SingleElementProcessorGraphElement storeTeamSidesModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(storeKickoffEventModule, null, "storeKickoffEventModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> subElementsKickoffEventFilterModuleGraphElement = new LinkedList<>();
            subElementsKickoffEventFilterModuleGraphElement.add(storeTeamSidesModuleGraphElement);
            SingleElementProcessorGraph.SingleElementProcessorGraphElement kickoffEventFilterModuleGraphElement = new SingleElementProcessorGraph.SingleElementProcessorGraphElement(kickoffEventFilterModule, subElementsKickoffEventFilterModuleGraphElement, "kickoffEventFilterModule");

            List<SingleElementProcessorGraph.SingleElementProcessorGraphElement> startElementsProcessGraph = new LinkedList<>();
            startElementsProcessGraph.add(ballFilterModuleGraphElement);
            startElementsProcessGraph.add(kickoffEventFilterModuleGraphElement);

            this.singleElementProcessorGraph = new SingleElementProcessorGraph(startElementsProcessGraph);

            /* Generated with http://asciiflow.com/
                      +                             +
                      | fieldObjectState,           | fieldObjectState,
                      | kickoffEvent                | kickoffEvent
                      |                             |
              +-------v--------+        +-----------v------------+
              |ballFilterModule|        |kickoffEventFilterModule|
              +-------+--------+        +-----------+------------+
                      |                             |
                      | fieldObjectState            | kickoffEvent
                      | (only ball)                 |
                      |                             |
                 +----v-----+           +-----------v-----------+
                 |timeModule|           |storeKickoffEventModule|
                 +----+-----+           +-----------------------+
                      |
                      | matchTimeProgressEvent
                      v
             */
        } catch (ConfigException | NumberFormatException | Schema.SchemaException | FilterModule.FilterException e) {
            logger.error("Caught exception during initialization.", e);
            System.exit(1);
        }
    }
}
