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

package ch.unibas.dmi.dbis.streamTeam.modules.generic;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.InternalActiveKeysStreamElement;
import ch.unibas.dmi.dbis.streamTeam.modules.WindowProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * Module for performing the window() part of activeKeys handling which is required to emit periodically an internalActiveKeys stream element for every active key which can be used in the SingleElementProcessorGraph of a StreamTeam worker (e.g., to periodically generate statistics).
 * Generates an internalActiveKeys stream element for every active key when window() is triggered.
 * Removes inactive keys from the activeKeys list.
 */
public class ActiveKeysWindowProcessorModule implements WindowProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(ActiveKeysWindowProcessorModule.class);

    /**
     * Maximum age in milliseconds before an inactive key gets removed from the activeKeys list
     */
    private final long activeTimeThreshold;

    /**
     * SingleValueStore that contains the activeKeys list
     */
    private final SingleValueStore<ArrayList<String>> activeKeysStore;

    /**
     * SingleValueStore that contains the processing timestamp of the last processed data stream element (for every key) (this is automatically the maximum)
     */
    private final SingleValueStore<Long> lastProcessingTimestampStore;

    /**
     * SingleValueStore that contains the maximum generation timestamp of all processed data stream elements (for every key)
     */
    private final SingleValueStore<Long> maxGenerationTimestampStore;

    /**
     * ActiveKeysWindowProcessorModule constructor.
     *
     * @param activeTimeThreshold          Maximum age in milliseconds before an inactive key gets removed from the activeKeys list
     * @param activeKeysStore              SingleValueStore that contains the activeKeys list
     * @param lastProcessingTimestampStore SingleValueStore that contains the processing timestamp of the last processed data stream element (for every key) (this is automatically the maximum)
     * @param maxGenerationTimestampStore  SingleValueStore that contains the maximum generation timestamp of all processed data stream elements (for every key)
     */
    public ActiveKeysWindowProcessorModule(long activeTimeThreshold, SingleValueStore<ArrayList<String>> activeKeysStore, SingleValueStore<Long> lastProcessingTimestampStore, SingleValueStore<Long> maxGenerationTimestampStore) {
        this.activeTimeThreshold = activeTimeThreshold;
        this.activeKeysStore = activeKeysStore;
        this.lastProcessingTimestampStore = lastProcessingTimestampStore;
        this.maxGenerationTimestampStore = maxGenerationTimestampStore;
    }

    /**
     * Generates an internalActiveKeys stream element for every active key when window() is triggered.
     * Removes inactive keys from the activeKeys list.
     *
     * @return internalActiveKeys stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> window() {
        logger.info("window() called");
        long currentTime = System.currentTimeMillis();

        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        try {
            ArrayList<String> oldActiveKeyList = this.activeKeysStore.get("all", Schema.STATIC_INNER_KEY);
            ArrayList<String> newActiveKeyList = new ArrayList<>();

            if (oldActiveKeyList != null) {
                for (String key : oldActiveKeyList) {
                    long lastProcessingTimestamp = this.lastProcessingTimestampStore.get(key, Schema.STATIC_INNER_KEY);
                    if (currentTime - lastProcessingTimestamp <= this.activeTimeThreshold) {
                        newActiveKeyList.add(key);
                        Long maxGenerationTimestamp = this.maxGenerationTimestampStore.get(key, Schema.STATIC_INNER_KEY);
                        if (maxGenerationTimestamp != null) {
                            outputList.add(InternalActiveKeysStreamElement.generateInternalActiveKeysStreamElement(key, maxGenerationTimestamp));
                        } else {
                            logger.error("Cannot emit data stream element for key={} since the maxGenerationTimestampStore does not contain a maximum generation timestamp for this key.", key);
                        }
                    }
                }
            }

            this.activeKeysStore.put("all", Schema.STATIC_INNER_KEY, newActiveKeyList);
        } catch (AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during window call: ", e);
        }
        return outputList;
    }
}
