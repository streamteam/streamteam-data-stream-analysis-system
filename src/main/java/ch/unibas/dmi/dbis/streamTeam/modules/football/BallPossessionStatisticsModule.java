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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionStatisticsStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Module for generating ballPossessionStatistics stream elements.
 * Assumes that the fieldObjectStateTsHistoryStore is filled by a StoreModule and the lastPlayerInPossessionStore is filled by a BallPossessionChangeDetectionModule.
 * Further assumes to receive only activeKeys data stream elements.
 * Generates ballPossessionStatistics stream elements (one for every statistics item) when it receives an activeKeys stream element.
 */
public class BallPossessionStatisticsModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(BallPossessionStatisticsModule.class);

    /**
     * A list of statistics items (players and teams) for which ball possession statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * The ball
     */
    private final ObjectInfo ball;

    /**
     * HistoryStore that contains the timestamps of the last fieldObjectState stream elements (filled by a StoreModule)
     */
    private final HistoryStore<Long> fieldObjectStateTsHistoryStore;

    /**
     * SingleValueStore that contains the identifier of the last player that has been in ball possession
     */
    private final SingleValueStore<String> playerInBallPossessionStore;

    /**
     * SingleValueStore that contains the identifier of the last team that has been in ball possession
     */
    private final SingleValueStore<String> teamInBallPossessionStore;

    /**
     * SingleValueStore that contains the time (in s) of ball possession per player/team
     */
    private final SingleValueStore<Long> ballPossessionTimeStore;


    /**
     * BallPossessionStatisticsModule constructor.
     *
     * @param statisticsItemInfos            A list of statistics items (players and teams) for which ball possession statistics should be sent
     * @param ball                           The ball
     * @param fieldObjectStateTsHistoryStore HistoryStore that contains the timestamps of the last fieldObjectState data stream elements (filled by a StoreModule)
     * @param playerInBallPossessionStore    SingleValueStore that contains the identifier of the last player that has been in ball possession
     * @param teamInBallPossessionStore      SingleValueStore that contains the identifier of the last team that has been in ball possession
     * @param ballPossessionTimeStore        SingleValueStore that contains the time (in s) of ball possession per player/team
     */
    public BallPossessionStatisticsModule(List<StatisticsItemInfo> statisticsItemInfos, ObjectInfo ball, HistoryStore<Long> fieldObjectStateTsHistoryStore, SingleValueStore<String> playerInBallPossessionStore, SingleValueStore<String> teamInBallPossessionStore, SingleValueStore<Long> ballPossessionTimeStore) {
        this.statisticsItemInfos = statisticsItemInfos;
        this.ball = ball;
        this.fieldObjectStateTsHistoryStore = fieldObjectStateTsHistoryStore;
        this.playerInBallPossessionStore = playerInBallPossessionStore;
        this.teamInBallPossessionStore = teamInBallPossessionStore;
        this.ballPossessionTimeStore = ballPossessionTimeStore;
    }

    /**
     * Generates a ballPossessionStatistics stream element for every statistics item when it receives an internalActiveKeys stream element (that specifies the match).
     * Assumes that tsHistoryStore is filled by a StoreModule and lastPlayerInPossessionStore is filled by a BallPossessionChangeDetectionModule.
     * Further assumes to receive only internalActiveKeys stream elements.
     *
     * @param inputDataStreamElement internalActiveKeys stream element
     * @return ballPossessionStatistics stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        String matchId = inputDataStreamElement.getKey();

        try {
            Long lastBallFieldObjectStateTs = this.fieldObjectStateTsHistoryStore.getLatest(matchId, this.ball.getObjectId());
            if (lastBallFieldObjectStateTs == null) {
                throw new BallPossessionStatisticsException("Cannot generate ball possession statistics: The ts history stores for the ball are not filled sufficiently (by a StoreModule).");
            } else {
                // Update ball possession statistics if some player is currently in in possession of the ball
                String lastPlayerInPossession = this.playerInBallPossessionStore.get(matchId, Schema.STATIC_INNER_KEY);
                String lastTeamInPossession = this.teamInBallPossessionStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (lastPlayerInPossession != null && !lastPlayerInPossession.equals("null") && lastTeamInPossession != null && !lastTeamInPossession.equals("null")) {
                    this.ballPossessionTimeStore.increase(matchId, lastPlayerInPossession, 1L);
                    this.ballPossessionTimeStore.increase(matchId, lastTeamInPossession, 1L);
                }

                // Generate new ballPossessionStatistics stream elements
                outputList.addAll(createBallPossessionStatisticsDataStreamElements(inputDataStreamElement.getGenerationTimestamp(), matchId));
            }

        } catch (SingleValueStore.SingleValueStoreException | BallPossessionStatisticsException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }
        return outputList;
    }

    /**
     * Creates ballPossessionStatistics stream elements for each statistics item.
     *
     * @param ts      Timestamp
     * @param matchId Match identifier
     * @return ballPossessionStatistics stream elements
     */
    private List<BallPossessionStatisticsStreamElement> createBallPossessionStatisticsDataStreamElements(long ts, String matchId) throws SingleValueStore.SingleValueStoreException, AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement {
        List<BallPossessionStatisticsStreamElement> ballPossessionStatisticsStreamElements = new LinkedList<>();

        Map<String, Long> ballPossessionTimes = new HashMap<>();
        long totalTime = 0;
        for (StatisticsItemInfo statisticsItemInfos : this.statisticsItemInfos) {
            long time = this.ballPossessionTimeStore.getLong(matchId, statisticsItemInfos.getUniqueInnerKeyForStore());
            ballPossessionTimes.put(statisticsItemInfos.getUniqueInnerKeyForStore(), time);
            if (statisticsItemInfos.getObjectId() == null) {
                totalTime += time;
            }
        }

        for (StatisticsItemInfo statisticsItemInfos : this.statisticsItemInfos) {
            long time = ballPossessionTimes.get(statisticsItemInfos.getUniqueInnerKeyForStore());
            double percentage;
            if (totalTime == 0) {
                percentage = 0.0;
            } else {
                percentage = ((double) time) / totalTime;
            }

            ballPossessionStatisticsStreamElements.add(BallPossessionStatisticsStreamElement.generateBallPossessionStatisticsStreamElement(matchId, ts, statisticsItemInfos, time, percentage));
        }

        return ballPossessionStatisticsStreamElements;
    }

    /**
     * Indicates that the ball possession statistics could not be be generated.
     */
    public static class BallPossessionStatisticsException extends Exception {

        /**
         * BallPossessionStatisticsException constructor.
         *
         * @param msg Message that explains the problem
         */
        public BallPossessionStatisticsException(String msg) {
            super(msg);
        }
    }
}
