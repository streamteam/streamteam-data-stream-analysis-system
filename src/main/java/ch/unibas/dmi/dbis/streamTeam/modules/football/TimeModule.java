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

package ch.unibas.dmi.dbis.streamTeam.modules.football;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchTimeProgressEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating matchTimeProgressEvent stream elements.
 * Assumes that the kickoffEventTsHistoryStore is filled by a StoreModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the ball.
 * Generates a matchTimeProgressEvent stream element every second after the first kickoff.
 */
public class TimeModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(TimeModule.class);

    /**
     * HistoryStore that contains the timestamps of all (last kickoffEventTsHistoryLength) kickoffEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<Long> kickoffEventTsHistoryStore;

    /**
     * SingleValueStore that contains the timestamp of the last matchTimeProgressEvent stream element
     */
    private final SingleValueStore<Long> lastTimeInSStore;

    /**
     * TimeModule constructor.
     *
     * @param kickoffEventTsHistoryStore HistoryStore that contains the timestamps of all (last kickoffEventTsHistoryLength) kickoffEvent stream elements (filled by a StoreModule)
     * @param lastTimeInSStore           SingleValueStore that contains the timestamp of the last matchTimeProgressEvent stream element
     */
    public TimeModule(HistoryStore<Long> kickoffEventTsHistoryStore, SingleValueStore<Long> lastTimeInSStore) {
        this.kickoffEventTsHistoryStore = kickoffEventTsHistoryStore;
        this.lastTimeInSStore = lastTimeInSStore;
    }

    /**
     * Generates a matchTimeProgressEvent stream element every second after the first kickoff.
     * Assumes to process only fieldObjectState stream elements for the ball.
     *
     * @param inputDataStreamElement fieldObjectState stream element for the ball
     * @return matchTimeProgressEvent stream element
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        try {
            List<Long> listOfKickoffEventTs = this.kickoffEventTsHistoryStore.getList(inputDataStreamElement);
            if (listOfKickoffEventTs == null) {
                logger.info("Cannot calculate time: KickoffEvent ts history store is empty.");
            } else {
                long latestKickoffEventTs = listOfKickoffEventTs.get(listOfKickoffEventTs.size() - 1);
                long curTs = inputDataStreamElement.getGenerationTimestamp();

                long tsDiff = curTs - latestKickoffEventTs;
                long timeInS = tsDiff / 1000;
                // TODO: Add logic for half-time break. (Note that also some of the statistics such as the ballPossessionStatistics have to regard the half-time break if a full match instead of only the first halftime has to be analyzed)

                if (timeInS < 0) {
                    logger.info("Negative time in s.");
                } else {
                    Long lastTimeInS = this.lastTimeInSStore.get(inputDataStreamElement);
                    if (lastTimeInS == null || lastTimeInS < timeInS) {
                        this.lastTimeInSStore.put(inputDataStreamElement, timeInS);
                        outputList.add(MatchTimeProgressEventStreamElement.generateMatchTimeProgressEventStreamElement(inputDataStreamElement.getKey(), curTs, timeInS));
                    }
                }
            }
        } catch (NumberFormatException | Schema.SchemaException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }
        return outputList;
    }
}
