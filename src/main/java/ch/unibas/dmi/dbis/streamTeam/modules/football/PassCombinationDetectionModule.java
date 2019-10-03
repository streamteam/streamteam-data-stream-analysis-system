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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.DoublePassEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.PassSequenceEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.PassSequenceStatisticsStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.SuccessfulPassEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.GroupInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.StatisticsItemInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Module for generating passSequenceEvent, doublePassEvent, and passSequenceStatistics stream elements.
 * Assumes that the successfulPassTsHistoryStore, the successfulPassTeamIdHistoryStore, the successfulPassKickPlayerIdHistoryStore, the successfulPassReceivePlayerIdHistoryStore, the successfulPassKickPositionHistory, the successfulPassReceivePositionHistory, the interceptionTsStore, the misplacedPassTsStore, the clearanceTsStore, and the ballLeftFieldTsStore are filled by StoreModules.
 * Further assumes that the input is filtered beforehand and contains only successfulPassEvent stream elements.
 * Generates a passSequenceEvent when there is an uninterrupted sequence of successful passes (min 2) and a doublePassEvent when there is an uninterrupted double pass.
 * Each doublePassEvent is also a passSequenceEvent.
 * Moreover, generates a passSequenceStatistics when a pass sequence is detected.
 */
public class PassCombinationDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PassCombinationDetectionModule.class);

    /**
     * HistoryStore that contains the timestamps of the last successfulPassEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<Long> successfulPassTsHistoryStore;

    /**
     * HistoryStore that contains the teamIds of the last successfulPassEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<String> successfulPassTeamIdHistoryStore;

    /**
     * HistoryStore that contains the kickPlayerIds of the last successfulPassEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<String> successfulPassKickPlayerIdHistoryStore;

    /**
     * HistoryStore that contains the receivePlayerIds of the last successfulPassEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<String> successfulPassReceivePlayerIdHistoryStore;

    /**
     * HistoryStore that contains the kick positions of the last successfulPassEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<Geometry.Vector> successfulPassKickPositionHistoryStore;

    /**
     * HistoryStore that contains the receive positions of the last successfulPassEvent stream elements (filled by a StoreModule)
     */
    private final HistoryStore<Geometry.Vector> successfulPassReceivePositionHistoryStore;

    /**
     * SingleValueStore that contains the ts of the latest interceptionEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Long> interceptionTsStore;

    /**
     * SingleValueStore that contains the ts of the latest misplacedPassEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Long> misplacedPassTsStore;

    /**
     * SingleValueStore that contains the ts of the latest clearanceEvent stream element (filled by a StoreModule)
     */
    private final SingleValueStore<Long> clearanceTsStore;

    /**
     * SingleValueStore that contains the ts of the latest areaEvent stream element with objectId[0] == "BALL", payload.areaId == "field", and payload.inArea == false (filled by a StoreModule)
     */
    private final SingleValueStore<Long> ballLeftFieldTsStore;

    /**
     * SingleValueStore that contains the ts of the first pass in the last generated passSequenceEvent
     */
    private final SingleValueStore<Long> firstTsOfLastPassSequenceStore;

    /**
     * SingleValueStore that contains the number of (distinct) pass sequences per player/team
     */
    private final SingleValueStore<Long> numPassSequencesStore;

    /**
     * SingleValueStore that contains the maximum pass sequence length per player/team
     */
    private final SingleValueStore<Long> maxPassSequenceLengthStore;

    /**
     * SingleValueStore that contains the sum of the lengths of all pass sequences (> 2 passes) per player/team
     */
    private final SingleValueStore<Long> sumPassSequenceLengthStore;

    /**
     * SingleValueStore that contains the number of double passes per player/team
     */
    private final SingleValueStore<Long> numDoublePassesStore;

    /**
     * Maximum time between two subsequent passes in a pass sequence
     */
    private final long maxTimeBetweenPasses;

    /**
     * A list of statistics items (players and teams) for which pass sequence statistics should be sent
     */
    private final List<StatisticsItemInfo> statisticsItemInfos;

    /**
     * SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    private final SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore;

    /**
     * PassCombinationDetectionModule constructor.
     *
     * @param statisticsItemInfos                         A list of statistics items (players and teams) for which pass sequence statistics should be sent
     * @param maxTimeBetweenPasses                        Maximum time between two subsequent passes in a pass sequence
     * @param successfulPassTsHistoryStore                HistoryStore that contains the timestamps of the last successfulPassEvent stream elements (filled by a StoreModule)
     * @param successfulPassTeamIdHistoryStore            HistoryStore that contains the teamIds of the last successfulPassEvent stream elements (filled by a StoreModule)
     * @param successfulPassKickPlayerIdHistoryStore      HistoryStore that contains the kickPlayerIds of the last successfulPassEvent stream elements (filled by a StoreModule)
     * @param successfulPassReceivePlayerIdHistoryStore   HistoryStore that contains the receivePlayerIds of the last successfulPassEvent stream elements (filled by a StoreModule)
     * @param successfulPassKickPositionHistoryStore      HistoryStore that contains the kick positions of the last successfulPassEvent stream elements (filled by a StoreModule)
     * @param successfulPassReceivePositionHistoryStore   HistoryStore that contains the receive positions of the last successfulPassEvent stream elements (filled by a StoreModule)
     * @param interceptionTsStore                         SingleValueStore that contains the ts of the latest interceptionEvent stream element (filled by a StoreModule)
     * @param misplacedPassTsStore                        SingleValueStore that contains the ts of the latest misplacedPassEvent stream element (filled by a StoreModule)
     * @param clearanceTsStore                            SingleValueStore that contains the ts of the latest clearanceEvent stream element (filled by a StoreModule)
     * @param ballLeftFieldTsStore                        SingleValueStore that contains the ts of the latest areaEvent stream element with objectId[0] == "BALL", payload.areaId == "field", and payload.inArea == false (filled by a StoreModule)
     * @param firstTsOfLastPassSequenceStore              SingleValueStore that contains the ts of the first pass in the last generated passSequenceEvent
     * @param numPassSequencesStore                       SingleValueStore that contains the number of (distinct) pass sequences per player/team
     * @param maxPassSequenceLengthStore                  SingleValueStore that contains the maximum pass sequence length per player/team
     * @param sumPassSequenceLengthStore                  SingleValueStore that contains the sum of the lengths of all pass sequences (> 2 passes) per player/team
     * @param numDoublePassesStore                        SingleValueStore that contains the number of double passes per player/team
     * @param hasSentInitialStatisticsStreamElementsStore SingleValueStore that contains the information if the initial statistics stream elements have already been sent
     */
    public PassCombinationDetectionModule(List<StatisticsItemInfo> statisticsItemInfos, long maxTimeBetweenPasses, HistoryStore<Long> successfulPassTsHistoryStore, HistoryStore<String> successfulPassTeamIdHistoryStore, HistoryStore<String> successfulPassKickPlayerIdHistoryStore, HistoryStore<String> successfulPassReceivePlayerIdHistoryStore, HistoryStore<Geometry.Vector> successfulPassKickPositionHistoryStore, HistoryStore<Geometry.Vector> successfulPassReceivePositionHistoryStore, SingleValueStore<Long> interceptionTsStore, SingleValueStore<Long> misplacedPassTsStore, SingleValueStore<Long> clearanceTsStore, SingleValueStore<Long> ballLeftFieldTsStore, SingleValueStore<Long> firstTsOfLastPassSequenceStore, SingleValueStore<Long> numPassSequencesStore, SingleValueStore<Long> maxPassSequenceLengthStore, SingleValueStore<Long> sumPassSequenceLengthStore, SingleValueStore<Long> numDoublePassesStore, SingleValueStore<Boolean> hasSentInitialStatisticsStreamElementsStore) {
        this.statisticsItemInfos = statisticsItemInfos;
        this.maxTimeBetweenPasses = maxTimeBetweenPasses;
        this.successfulPassTsHistoryStore = successfulPassTsHistoryStore;
        this.successfulPassTeamIdHistoryStore = successfulPassTeamIdHistoryStore;
        this.successfulPassKickPlayerIdHistoryStore = successfulPassKickPlayerIdHistoryStore;
        this.successfulPassReceivePlayerIdHistoryStore = successfulPassReceivePlayerIdHistoryStore;
        this.successfulPassKickPositionHistoryStore = successfulPassKickPositionHistoryStore;
        this.successfulPassReceivePositionHistoryStore = successfulPassReceivePositionHistoryStore;
        this.interceptionTsStore = interceptionTsStore;
        this.misplacedPassTsStore = misplacedPassTsStore;
        this.clearanceTsStore = clearanceTsStore;
        this.ballLeftFieldTsStore = ballLeftFieldTsStore;
        this.firstTsOfLastPassSequenceStore = firstTsOfLastPassSequenceStore;
        this.numPassSequencesStore = numPassSequencesStore;
        this.maxPassSequenceLengthStore = maxPassSequenceLengthStore;
        this.sumPassSequenceLengthStore = sumPassSequenceLengthStore;
        this.numDoublePassesStore = numDoublePassesStore;
        this.hasSentInitialStatisticsStreamElementsStore = hasSentInitialStatisticsStreamElementsStore;
    }

    /**
     * Generates a passSequenceEvent and a passSequenceStatistics for every detected pass sequence and a doublePassEvent for every detected double pass.
     * Assumes to process only successfulPassEvent stream elements.
     *
     * @param inputDataStreamElement successfulPassEvent stream element
     * @return passSequenceEvent, passSequenceStatistics, and doublePassEvent stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        SuccessfulPassEventStreamElement successfulPassEventStreamElement = (SuccessfulPassEventStreamElement) inputDataStreamElement;

        String matchId = successfulPassEventStreamElement.getKey();
        long ts = successfulPassEventStreamElement.getGenerationTimestamp();

        try {
            // Produce first setPlayStatistics stream elements for this match (if this has not been done yet)
            if (!this.hasSentInitialStatisticsStreamElementsStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                for (StatisticsItemInfo statisticsItemInfo : this.statisticsItemInfos) {
                    outputList.add(createPassSequenceStatisticsDataStreamElement(ts, matchId, statisticsItemInfo));
                }
            }

            // Get the list of ts, teamIds, kickPlayerIds, receivePlayerIds, kick positions, and receive positions
            List<Long> successfulPassTsList = this.successfulPassTsHistoryStore.getList(matchId, Schema.STATIC_INNER_KEY);
            List<String> successfulPassTeamIdList = this.successfulPassTeamIdHistoryStore.getList(matchId, Schema.STATIC_INNER_KEY);
            List<String> successfulPassKickPlayerIdList = this.successfulPassKickPlayerIdHistoryStore.getList(matchId, Schema.STATIC_INNER_KEY);
            List<String> successfulPassReceivePlayerIdList = this.successfulPassReceivePlayerIdHistoryStore.getList(matchId, Schema.STATIC_INNER_KEY);
            List<Geometry.Vector> successfulPassKickPosList = this.successfulPassKickPositionHistoryStore.getList(matchId, Schema.STATIC_INNER_KEY);
            List<Geometry.Vector> successfulPassReceivePosList = this.successfulPassReceivePositionHistoryStore.getList(matchId, Schema.STATIC_INNER_KEY);

            if (successfulPassTeamIdList != null && successfulPassKickPlayerIdList != null && successfulPassReceivePlayerIdList != null && successfulPassKickPosList != null && successfulPassReceivePosList != null) {
                // Get the timestamp of the last interceptionEvent, misplacedPassEvent, and clearanceEvent
                Long lastInterceptionTs = this.interceptionTsStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                Long lastMisplacedPassTs = this.misplacedPassTsStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                Long lastClearanceTs = this.clearanceTsStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                Long ballLeftFieldTs = this.ballLeftFieldTsStore.getLong(matchId, Schema.STATIC_INNER_KEY);

                // Generate the current pass sequence using the data retrieved from the histories and single value stores
                Deque<Pair<ObjectInfo, ObjectInfo>> passSequence = generatePassSequence(successfulPassTsList, successfulPassTeamIdList, successfulPassKickPlayerIdList, successfulPassReceivePlayerIdList, successfulPassKickPosList, successfulPassReceivePosList, lastInterceptionTs, lastMisplacedPassTs, lastClearanceTs, ballLeftFieldTs);

                // Get the teamId from the last successfulPassEvent
                String teamId = successfulPassEventStreamElement.getTeamId();

                // Update pass sequence statistics and generate passSequenceEvent, doublePassEvent, and passSequenceStatistics stream elements
                outputList.addAll(generateDataStreamElementsAndUpdateStatistics(ts, matchId, teamId, passSequence, successfulPassTsList, successfulPassKickPlayerIdList, successfulPassReceivePlayerIdList));
            } else {
                throw new PassCombinationException("Cannot detect pass combinations: One of the required HistoryStores is empty.");
            }
        } catch (PassCombinationException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        this.hasSentInitialStatisticsStreamElementsStore.put(matchId, Schema.STATIC_INNER_KEY, new Boolean(true)); // set hasSentInitialStatisticsStreamElements to true here since there might haven been an exception which cause not sending the initial statistics stream elements

        return outputList;
    }

    /**
     * Generate the current pass sequence using the data retrieved from the histories and single value stores.
     *
     * @param successfulPassTsList              List of timestamps of the last successfulPassEvents
     * @param successfulPassTeamIdList          List of teamIds of the last successfulPassEvents
     * @param successfulPassKickPlayerIdList    List of kickPlayerIds of the last successfulPassEvents
     * @param successfulPassReceivePlayerIdList List of receivePlayerIds of the last successfulPassEvents
     * @param successfulPassKickPosList         List of kick positions of the last successfulPassEvents
     * @param successfulPassReceivePosList      List of receive positions of the last successfulPassEvents
     * @param lastInterceptionTs                Timestamp of the last interceptionEvent
     * @param lastMisplacedPassTs               Timestamp of the last misplacedPassEvent
     * @param lastClearanceTs                   Timestamp of the last clearanceEvent
     * @param ballLeftFieldTs                   Timestamp of the last ball left field event (areaEvent with objectId[0] == "BALL", payload.areaId == "field", and payload.inArea == false)
     * @return Stack of passes (top element = oldest pass in the sequence)
     */
    private Deque<Pair<ObjectInfo, ObjectInfo>> generatePassSequence(List<Long> successfulPassTsList, List<String> successfulPassTeamIdList, List<String> successfulPassKickPlayerIdList, List<String> successfulPassReceivePlayerIdList, List<Geometry.Vector> successfulPassKickPosList, List<Geometry.Vector> successfulPassReceivePosList, Long lastInterceptionTs, Long lastMisplacedPassTs, Long lastClearanceTs, Long ballLeftFieldTs) {
        Deque<Pair<ObjectInfo, ObjectInfo>> passSequence = new ArrayDeque<>();
        String sequenceTeam = null;
        int passNum = 0;

        PASSLOOP:
        while (passNum < successfulPassTeamIdList.size()) { // larger index = older pass --> traverse through passes in reverse order
            // Stop if the pass team changes (all passes in a pass sequence have to be of the same team)
            String curTeam = successfulPassTeamIdList.get(passNum);
            if (sequenceTeam == null) {
                sequenceTeam = curTeam;
            }
            if (!curTeam.equals(sequenceTeam)) {
                break PASSLOOP;
            }

            // Get the kickPlayerId and the receivePlayerId of the current pass
            String kickPlayerId = successfulPassKickPlayerIdList.get(passNum);
            String receivePlayerId = successfulPassReceivePlayerIdList.get(passNum);

            if (passNum > 0) { // if the current pass is not the first pass in the history
                // Stop if the pause between two subsequent passes is too long
                long curPassTs = successfulPassTsList.get(passNum);
                long nextPassTs = successfulPassTsList.get(passNum - 1);
                long timeUntilNextPass = nextPassTs - curPassTs;
                if (timeUntilNextPass > this.maxTimeBetweenPasses) {
                    break PASSLOOP;
                }

                // Stop if the receive player of the current pass is not the kick player of the next pass
                String nextKickPlayerId = successfulPassKickPlayerIdList.get(passNum - 1);
                if (!receivePlayerId.equals(nextKickPlayerId)) {
                    break PASSLOOP;
                }

                // Stop if there was an interceptionEvent between the current and the next pass
                if (lastInterceptionTs > curPassTs) {
                    break PASSLOOP;
                }

                // Stop if there was a misplacedPassEvent between the current and the next pass
                if (lastMisplacedPassTs > curPassTs) {
                    break PASSLOOP;
                }

                // Stop if there was a clearanceEvent between the current and the next pass
                if (lastClearanceTs > curPassTs) {
                    break PASSLOOP;
                }

                // Stop if the ball has left the field between the current and the next pass
                if (ballLeftFieldTs > curPassTs) {
                    break PASSLOOP;
                }
            }

            // If not stopped -> Add the current pass to the pass sequence
            ObjectInfo kickPlayer = new ObjectInfo(kickPlayerId, curTeam, successfulPassKickPosList.get(passNum));
            ObjectInfo receivePlayer = new ObjectInfo(receivePlayerId, curTeam, successfulPassReceivePosList.get(passNum));
            Pair<ObjectInfo, ObjectInfo> pass = Pair.with(kickPlayer, receivePlayer);
            passSequence.push(pass);

            passNum++;
        }

        return passSequence;
    }

    /**
     * Update pass sequence statistics and generate passSequenceEvent, doublePassEvent and passSequenceStatistics stream elements.
     *
     * @param ts                                Timestamp of the last successfulPassEvent
     * @param matchId                           Match identifier
     * @param teamId                            Team identifier of the last successfulPassEvent
     * @param passSequence                      Pass sequence (Stack: top element = oldest pass in the sequence)
     * @param successfulPassTsList              List of timestamps of the last successfulPassEvents
     * @param successfulPassKickPlayerIdList    List of kickPlayerIds of the last successfulPassEvents
     * @param successfulPassReceivePlayerIdList List of receivePlayerIds of the last successfulPassEvents
     * @return Generated passSequenceEvent, doublePassEvent, and passSequenceStatistics stream elements
     * @throws PassCombinationException Thrown if the pass sequence statistics cannot be updated or the passSequenceStatistics stream element could not be generated
     */
    private List<AbstractImmutableDataStreamElement> generateDataStreamElementsAndUpdateStatistics(long ts, String matchId, String teamId, Deque<Pair<ObjectInfo, ObjectInfo>> passSequence, List<Long> successfulPassTsList, List<String> successfulPassKickPlayerIdList, List<String> successfulPassReceivePlayerIdList) throws PassCombinationException {
        try {
            List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

            // Generate a set containing the teamId as well as the identifiers of all players who participated in the pass sequences as a kicker, receiver, or both
            Set<StatisticsItemInfo> participatingStatisticsItemsInfos = new HashSet<>();
            participatingStatisticsItemsInfos.add(new GroupInfo(teamId));
            for (Pair<ObjectInfo, ObjectInfo> pass : passSequence) {
                ObjectInfo kickPlayerInfoWithoutPos = new ObjectInfo(pass.getValue0().getObjectId(), pass.getValue0().getGroupId());
                ObjectInfo receivePlayerInfoWithoutPos = new ObjectInfo(pass.getValue1().getObjectId(), pass.getValue1().getGroupId());
                if (!participatingStatisticsItemsInfos.contains(kickPlayerInfoWithoutPos)) {
                    participatingStatisticsItemsInfos.add(kickPlayerInfoWithoutPos);
                }
                if (!participatingStatisticsItemsInfos.contains(receivePlayerInfoWithoutPos)) {
                    participatingStatisticsItemsInfos.add(receivePlayerInfoWithoutPos);
                }
            }

            int passSequenceLength = passSequence.size();
            if (passSequenceLength >= 2) { // Pass sequence = min 2 passes
                // Generate a passSequenceEvent stream element
                outputList.add(PassSequenceEventStreamElement.generatePassSequenceEventStreamElement(matchId, ts, teamId, passSequence));

                // Update number of pass sequences and sum of pass sequence lengths statistics
                for (StatisticsItemInfo participatingStatisticsItem : participatingStatisticsItemsInfos) {
                    // If this pass sequence is new for this object (team or player) --> Increase number of pass sequences by 1 and increase the sum of pass sequence length by the length of the pass sequence
                    long firstTsOfCurrentPassSequence = successfulPassTsList.get(passSequenceLength - 1);
                    Long firstTsOfLastPassSequence = this.firstTsOfLastPassSequenceStore.get(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore());
                    if (firstTsOfLastPassSequence == null || firstTsOfCurrentPassSequence > firstTsOfLastPassSequence) {
                        this.firstTsOfLastPassSequenceStore.put(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore(), firstTsOfCurrentPassSequence);
                        this.numPassSequencesStore.increase(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore(), 1L);
                        this.sumPassSequenceLengthStore.increase(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore(), (long) passSequenceLength);
                    } else {
                        // Otherwise (not new for this object) --> Increase the sum of pass sequence length by 1 (i.e., no new pass sequence but the length of the latest pass sequence has increased by one)
                        this.sumPassSequenceLengthStore.increase(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore(), 1L);
                    }
                }

                // If double pass (e.g., A1 -> A2 -> A1) --> additionally generate doublePassEvent stream element and increase the number of double passes by 1
                if (passSequenceLength == 2 && successfulPassKickPlayerIdList.get(passSequenceLength - 1).equals(successfulPassReceivePlayerIdList.get(passSequenceLength - 2))) {
                    outputList.add(DoublePassEventStreamElement.generateDoublePassEventStreamElement(matchId, ts, teamId, passSequence));
                    for (StatisticsItemInfo participatingStatisticsItem : participatingStatisticsItemsInfos) {
                        this.numDoublePassesStore.increase(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore(), 1L);
                    }
                }

                for (StatisticsItemInfo participatingStatisticsItem : participatingStatisticsItemsInfos) {
                    // If the new pass sequence is longer then any pass sequence seen before for this object (team or player) --> Update maximum pass sequence length
                    long maxPassSequenceLength = this.maxPassSequenceLengthStore.getLong(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore());
                    if (passSequenceLength > maxPassSequenceLength) {
                        this.maxPassSequenceLengthStore.put(matchId, participatingStatisticsItem.getUniqueInnerKeyForStore(), (long) passSequenceLength);
                    }
                }

                // Generate a passSequenceStatistics stream element for each participating player (at least the average pass sequence length was updated as the value of the sumPassSequenceLengthStore has been increased)
                for (StatisticsItemInfo participatingStatisticsItem : participatingStatisticsItemsInfos) {
                    outputList.add(createPassSequenceStatisticsDataStreamElement(ts, matchId, participatingStatisticsItem));
                }
            }
            return outputList;
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassCombinationException("Cannot update and generate pass sequence statistics: " + e.toString());
        }
    }

    /**
     * Creates a passSequenceStatistics stream element for a statistics item (player or team).
     *
     * @param ts                 Timestamp
     * @param matchId            Match identifier
     * @param statisticsItemInfo StatisticsItemInfo
     * @return passSequenceStatistics stream element
     * @throws PassCombinationException Thrown if the passSequenceStatistics stream element could not be generated
     */
    private PassSequenceStatisticsStreamElement createPassSequenceStatisticsDataStreamElement(long ts, String matchId, StatisticsItemInfo statisticsItemInfo) throws PassCombinationException {
        try {
            long numPassSequences = this.numPassSequencesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long maxPassSequenceLength = this.maxPassSequenceLengthStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long sumPassSequenceLength = this.sumPassSequenceLengthStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());
            long numDoublePasses = this.numDoublePassesStore.getLong(matchId, statisticsItemInfo.getUniqueInnerKeyForStore());

            double avgPassSequenceLength;
            if (numPassSequences == 0) {
                avgPassSequenceLength = 0;
            } else {
                avgPassSequenceLength = ((double) sumPassSequenceLength) / numPassSequences;
            }


            return PassSequenceStatisticsStreamElement.generatePassSequenceStatisticsStreamElement(matchId, ts, statisticsItemInfo, numPassSequences, avgPassSequenceLength, maxPassSequenceLength, numDoublePasses);
        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement e) {
            throw new PassCombinationException("Cannot generate passSequenceStatistics stream element: " + e.toString());
        }
    }

    /**
     * Indicates that a pass sequence event or a double pass event could not be detected or that the pass sequence statistics could not be generated.
     */
    public static class PassCombinationException extends Exception {

        /**
         * PassCombinationException constructor.
         *
         * @param msg Message that explains the problem
         */
        public PassCombinationException(String msg) {
            super(msg);
        }
    }
}
