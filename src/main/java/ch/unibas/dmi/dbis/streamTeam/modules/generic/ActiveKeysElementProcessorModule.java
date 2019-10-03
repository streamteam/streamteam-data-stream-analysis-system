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
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * Module for performing the processElement() part of activeKeys handling which is required to emit periodically an internalActiveKeys stream element for every active key which can be used in the SingleElementProcessorGraph of a StreamTeam worker (e.g., to periodically generate statistics).
 * Does not produce output data stream elements.
 * Updates the last processing timestamp and the maximum generation timestamp for every key and adds new keys to the activeKeys list.
 */
public class ActiveKeysElementProcessorModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(ActiveKeysElementProcessorModule.class);

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
     * ActiveKeysElementProcessorModule constructor.
     *
     * @param activeKeysStore              SingleValueStore that contains the activeKeys list
     * @param lastProcessingTimestampStore SingleValueStore that contains the processing timestamp of the last processed data stream element (for every key) (this is automatically the maximum)
     * @param maxGenerationTimestampStore  SingleValueStore that contains the maximum generation timestamp of all processed data stream elements (for every key)
     */
    public ActiveKeysElementProcessorModule(SingleValueStore<ArrayList<String>> activeKeysStore, SingleValueStore<Long> lastProcessingTimestampStore, SingleValueStore<Long> maxGenerationTimestampStore) {
        this.activeKeysStore = activeKeysStore;
        this.lastProcessingTimestampStore = lastProcessingTimestampStore;
        this.maxGenerationTimestampStore = maxGenerationTimestampStore;
    }

    /**
     * Updates the last processing timestamp and the maximum generation timestamp and adds new keys to the activeKeys list.
     * Does not produce output data stream elements.
     *
     * @param inputDataStreamElement Input data stream element
     * @return Empty list
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        try {
            long processingTimestamp = inputDataStreamElement.getProcessingTimestamp();
            this.lastProcessingTimestampStore.put(inputDataStreamElement.getKey(), Schema.STATIC_INNER_KEY, processingTimestamp);

            long generationTimestamp = inputDataStreamElement.getGenerationTimestamp();
            Long maxGenerationTimestamp = this.maxGenerationTimestampStore.get(inputDataStreamElement.getKey(), Schema.STATIC_INNER_KEY);
            if (maxGenerationTimestamp == null || maxGenerationTimestamp < generationTimestamp) {
                this.maxGenerationTimestampStore.put(inputDataStreamElement.getKey(), Schema.STATIC_INNER_KEY, generationTimestamp);
            }
            // Note: Data stream elements are not guaranteed to be processed in order with respect to their generation timestamp. Therefore, the last generation timestamp is not necessarily the maximum generation timestamp.

            ArrayList<String> activeKeys = this.activeKeysStore.get("all", Schema.STATIC_INNER_KEY);
            if (!activeKeys.contains(inputDataStreamElement.getKey())) {
                activeKeys.add(inputDataStreamElement.getKey());
                this.activeKeysStore.put("all", Schema.STATIC_INNER_KEY, activeKeys);
            }
        } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return new LinkedList<>();
    }
}
