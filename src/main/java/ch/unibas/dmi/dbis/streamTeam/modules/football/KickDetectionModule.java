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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.FieldObjectStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.KickEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.football.helper.Packing;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Module for generating kickEvent stream elements.
 * Assumes that the fieldLengthStore, the positionStores, the leftTeamIdStore, the currentlyBallInLeftThirdStore, the currentlyBallInCenterThirdStore, the currentlyBallInRightThirdStore, the currentDuelPhaseStore, and the currentUnderPressurePhaseStore are filled by StoreModules and that the ballPossessionInformationStore is filled by the StoreBallPossessionInformationModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the ball.
 * Generates a kickEvent for every detected kick.
 */
public class KickDetectionModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(KickDetectionModule.class);

    /**
     * Threshold for the minimum distance for detecting a kick of the player in ball possession
     */
    public final double minKickDist;

    /**
     * Threshold for the maximum distance for detecting that there is no active kick of the player in ball possession
     */
    public final double maxBallbackDist;

    /**
     * SingleValueStore that contains the length of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Double> fieldLengthStore;

    /**
     * SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     */
    private final SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore;

    /**
     * SingleValueStore that contains the latest position (for every player) (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     */
    private final SingleValueStore<String> leftTeamIdStore;

    /**
     * SingleValueStore that contains information if the ball is currently in the left third of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInLeftThirdStore;

    /**
     * SingleValueStore that contains information if the ball is currently in the center third of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInCenterThirdStore;

    /**
     * SingleValueStore that contains information if the ball is currently in the right third of the field (filled by a StoreModule)
     */
    private final SingleValueStore<Boolean> currentlyBallInRightThirdStore;

    /**
     * SingleValueStore that contains the current duel phase (filled by a StoreModule)
     */
    private final SingleValueStore<NonAtomicEventPhase> currentDuelPhaseStore;

    /**
     * SingleValueStore that contains the current under pressure phase (filled by a StoreModule)
     */
    private final SingleValueStore<NonAtomicEventPhase> currentUnderPressurePhaseStore;

    /**
     * SingleValueStore that contains information if there is an active kick
     */
    private final SingleValueStore<Boolean> activeKickStore;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;


    /**
     * KickDetectionModule constructor.
     *
     * @param players                         A list of all players
     * @param minKickDist                     Threshold for the minimum distance for detecting a kick of the player in ball possession
     * @param maxBallbackDist                 Threshold for the maximum distance for detecting that there is no active kick of the player in ball possession
     * @param fieldLengthStore                SingleValueStore that contains the length of the field (filled by a StoreModule)
     * @param ballPossessionInformationStore  SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     * @param positionStore                   SingleValueStore that contains the latest position (for every player) (filled by a StoreModule)
     * @param leftTeamIdStore                 SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     * @param currentlyBallInLeftThirdStore   SingleValueStore that contains information if the ball is currently in the left third of the field (filled by a StoreModule)
     * @param currentlyBallInCenterThirdStore SingleValueStore that contains information if the ball is currently in the center third of the field (filled by a StoreModule)
     * @param currentlyBallInRightThirdStore  SingleValueStore that contains information if the ball is currently in the right third of the field (filled by a StoreModule)
     * @param currentDuelPhaseStore           SingleValueStore that contains the current duel phase (filled by a StoreModule)
     * @param currentUnderPressurePhaseStore  SingleValueStore that contains the current under pressure phase (filled by a StoreModule)
     * @param activeKickStore                 SingleValueStore that contains information if there is an active kick
     */
    public KickDetectionModule(List<ObjectInfo> players, double minKickDist, double maxBallbackDist, SingleValueStore<Double> fieldLengthStore, SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<String> leftTeamIdStore, SingleValueStore<Boolean> currentlyBallInLeftThirdStore, SingleValueStore<Boolean> currentlyBallInCenterThirdStore, SingleValueStore<Boolean> currentlyBallInRightThirdStore, SingleValueStore<NonAtomicEventPhase> currentDuelPhaseStore, SingleValueStore<NonAtomicEventPhase> currentUnderPressurePhaseStore, SingleValueStore<Boolean> activeKickStore) {
        this.players = players;
        this.minKickDist = minKickDist;
        this.maxBallbackDist = maxBallbackDist;
        this.fieldLengthStore = fieldLengthStore;
        this.ballPossessionInformationStore = ballPossessionInformationStore;
        this.positionStore = positionStore;
        this.leftTeamIdStore = leftTeamIdStore;
        this.currentlyBallInLeftThirdStore = currentlyBallInLeftThirdStore;
        this.currentlyBallInCenterThirdStore = currentlyBallInCenterThirdStore;
        this.currentlyBallInRightThirdStore = currentlyBallInRightThirdStore;
        this.currentDuelPhaseStore = currentDuelPhaseStore;
        this.currentUnderPressurePhaseStore = currentUnderPressurePhaseStore;
        this.activeKickStore = activeKickStore;
    }

    /**
     * Generates a kickEvent for every detected kick.
     * Assumes to process only fieldObjectState stream elements for the ball.
     *
     * @param inputDataStreamElement fieldObjectState stream element for the ball
     * @return kickEvent stream element if a new kick is detected
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement ballFieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        try {
            KickEventStreamElement kickEventDataStreamElement = detectKick(ballFieldObjectStateStreamElement);

            if (kickEventDataStreamElement != null) {
                outputList.add(kickEventDataStreamElement);
            }
        } catch (KickEventException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Detects kicks and generates a new kickEvent stream element if a kick is detected.
     *
     * @param ballFieldObjectStateStreamElement fieldObjectState data stream element for the ball
     * @return kickEvent stream element if a new kick is detected
     * @throws KickEventException Thrown if there was a problem during the kick detection
     */
    private KickEventStreamElement detectKick(FieldObjectStateStreamElement ballFieldObjectStateStreamElement) throws KickEventException {
        try {
            Geometry.Vector ballPos = ballFieldObjectStateStreamElement.getPosition();
            String matchId = ballFieldObjectStateStreamElement.getKey();

            StoreBallPossessionInformationModule.BallPossessionInformation ballPossessionInformation = this.ballPossessionInformationStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (ballPossessionInformation == null) {
                ballPossessionInformation = new StoreBallPossessionInformationModule.BallPossessionInformation(false, null, null);
            }

            if (ballPossessionInformation.isSomeoneInBallPossession()) { // if some player is in possession of the ball
                ObjectInfo playerInPossession = ObjectInfoFactoryAndModifier.createObjectInfoWithPositionFromSingleValueStore(matchId, ballPossessionInformation.getPlayerId(), ballPossessionInformation.getTeamId(), this.positionStore);

                double distance = Geometry.distance(ballPos, playerInPossession.getPosition());

                if (distance > this.minKickDist) {
                    Boolean isKickActive = this.activeKickStore.get(matchId, Schema.STATIC_INNER_KEY);
                    if (isKickActive == null || !isKickActive) {
                        this.activeKickStore.put(matchId, Schema.STATIC_INNER_KEY, true);

                        // Calculate numPlayersNearerToGoal
                        String leftTeamId = this.leftTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);
                        Double fieldLength = this.fieldLengthStore.get(matchId, Schema.STATIC_INNER_KEY);

                        if (fieldLength == null) {
                            throw new KickEventException("Cannot detect kick: The fieldLength single value store is not filled sufficiently (by a StoreModule).");
                        } else {
                            Geometry.Vector goalPosition;
                            if (playerInPossession.getGroupId().equals(leftTeamId)) {
                                goalPosition = new Geometry.Vector(fieldLength / 2, 0.0, 0.0);
                            } else {
                                goalPosition = new Geometry.Vector(-fieldLength / 2, 0.0, 0.0);
                            }
                            Set<Geometry.Vector> foreignTeamPlayerPositions = new HashSet<>();
                            for (ObjectInfo player : this.players) {
                                if (!player.getGroupId().equals(playerInPossession.getGroupId())) {
                                    ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
                                    foreignTeamPlayerPositions.add(player.getPosition());
                                }
                            }
                            int numPlayersNearerToGoal = Packing.calculateNumPlayersNearerToGoal(playerInPossession.getPosition(), goalPosition, foreignTeamPlayerPositions);

                            // Zone
                            String zone = "outside";
                            if (this.currentlyBallInLeftThirdStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                                zone = "left";
                            } else if (this.currentlyBallInCenterThirdStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                                zone = "center";
                            } else if (this.currentlyBallInRightThirdStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                                zone = "right";
                            }

                            // Active duel event
                            NonAtomicEventPhase duelPhase = this.currentDuelPhaseStore.get(matchId, Schema.STATIC_INNER_KEY);
                            boolean activeDuel = true;
                            if (duelPhase == null || duelPhase.equals(NonAtomicEventPhase.END)) {
                                activeDuel = false;
                            }

                            // Active under pressure event
                            NonAtomicEventPhase underPressurePhase = this.currentUnderPressurePhaseStore.get(matchId, Schema.STATIC_INNER_KEY);
                            boolean activeUnderPressure = true;
                            if (underPressurePhase == null || underPressurePhase.equals(NonAtomicEventPhase.END)) {
                                activeUnderPressure = false;
                            }

                            boolean attacked = activeDuel || activeUnderPressure;

                            logger.info("kickEvent with attacked = {}", attacked);
                            logger.info("activeDuel = {}, activeUnderPressure = {}", activeDuel, activeUnderPressure);

                            long ts = ballFieldObjectStateStreamElement.getGenerationTimestamp();

                            return KickEventStreamElement.generateKickEventStreamElement(matchId, ts, playerInPossession, ballPos, numPlayersNearerToGoal, attacked, zone);
                        }
                    }
                } else if (distance < this.maxBallbackDist) {
                    this.activeKickStore.put(matchId, Schema.STATIC_INNER_KEY, false);
                }
            }
        } catch (NumberFormatException | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement | StoreBallPossessionInformationModule.BallPossessionInformationException e) {
            throw new KickEventException("Cannot detect kick: " + e.toString());
        }

        return null;
    }

    /**
     * Indicates that a kick event could not be detected.
     */
    public static class KickEventException extends Exception {

        /**
         * KickEventException constructor.
         *
         * @param msg Message that explains the problem
         */
        public KickEventException(String msg) {
            super(msg);
        }
    }
}
