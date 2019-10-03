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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.PressingStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.UnderPressureEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating pressingState and underPressureEvent stream elements.
 * Assumes that the positionStore is filled by StorageModules, that the ballPossessionInformationStore is filled by the StoreBallPossessionInformationModule, and that the pressingIndexStore and the pressingTsStore are filled by the PressingCalculationModule.
 * Further assumes to receive only internalActiveKeys stream elements.
 * Generates a pressingState stream element when it receives an internalActiveKeys stream element.
 * Moreover, generates underPressureEvents while the player in ball possession is under high pressure.
 */
public class PressingSenderModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(PressingSenderModule.class);

    /**
     * Minimum pressing index value for detecting under pressure events
     */
    private final double minPressingIndexForUnderPressure;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * SingleValueStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     */
    private final SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore;

    /**
     * SingleValueStore that contains the pressing index value
     */
    private final SingleValueStore<Double> pressingIndexStore;

    /**
     * SingleValueStore that contains the timestamp of the latest fieldObjectState stream element for which the pressing index value was updated
     */
    private final SingleValueStore<Long> pressingTsStore;

    /**
     * SingleValueStore that contains the identifier of the player under pressure
     */
    private final SingleValueStore<String> playerUnderPressureStore;

    /**
     * SingleValueStore that contains the counter for the event identifiers of the underPressure events
     */
    private final SingleValueStore<Long> underPressureEventIdentifierCounterStore;

    /**
     * PressingSenderModule constructor.
     *
     * @param minPressingIndexForUnderPressure         Minimum pressing index value for detecting under pressure events
     * @param players                                  A list of all players
     * @param positionStore                            SingleValueStore that contains the latest position (for every player and the ball) (filled by a StoreModule)
     * @param ballPossessionInformationStore           SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     * @param pressingIndexStore                       SingleValueStore that contains the pressing index value
     * @param pressingTsStore                          SingleValueStore that contains the timestamp of the latest fieldObjectState stream element for which the pressing index value was updated
     * @param playerUnderPressureStore                 SingleValueStore that contains the identifier of the player under pressure
     * @param underPressureEventIdentifierCounterStore SingleValueStore that contains the counter for the event identifiers of the underPressure events
     */
    public PressingSenderModule(double minPressingIndexForUnderPressure, List<ObjectInfo> players, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore, SingleValueStore<Double> pressingIndexStore, SingleValueStore<Long> pressingTsStore, SingleValueStore<String> playerUnderPressureStore, SingleValueStore<Long> underPressureEventIdentifierCounterStore) {
        this.minPressingIndexForUnderPressure = minPressingIndexForUnderPressure;
        this.players = players;
        this.positionStore = positionStore;
        this.ballPossessionInformationStore = ballPossessionInformationStore;
        this.pressingIndexStore = pressingIndexStore;
        this.pressingTsStore = pressingTsStore;
        this.playerUnderPressureStore = playerUnderPressureStore;
        this.underPressureEventIdentifierCounterStore = underPressureEventIdentifierCounterStore;
    }

    /**
     * Generates a pressingState when it receives an internalActiveKeys stream element (that specifies the match).
     * Moreover, generates underPressureEvents while the player in ball possession is under high pressure.
     * Assumes to receive only internalActiveKeys stream elements.
     *
     * @param inputDataStreamElement internalActiveKeys stream element
     * @return pressingState and underPressureEvent stream elements
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        String matchId = inputDataStreamElement.getKey();

        try {
            Long pressingTs = this.pressingTsStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (pressingTs != null) {
                double pressingIndex = this.pressingIndexStore.getDouble(matchId, Schema.STATIC_INNER_KEY);

                // Get ball possession information from the single value store
                StoreBallPossessionInformationModule.BallPossessionInformation ballPossessionInformation = this.ballPossessionInformationStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (ballPossessionInformation == null) {
                    ballPossessionInformation = new StoreBallPossessionInformationModule.BallPossessionInformation(false, null, null);
                }

                // Get identifier of the player who is under pressure from the single value store
                String playerUnderPressureId = this.playerUnderPressureStore.get(matchId, Schema.STATIC_INNER_KEY);
                if (playerUnderPressureId != null && playerUnderPressureId.equals("null")) {
                    playerUnderPressureId = null;
                }

                // Check if there is an active under pressure event
                boolean activeUnderPressure = false;
                ObjectInfo playerUnderPressure = null;
                if (playerUnderPressureId != null) {
                    activeUnderPressure = true;
                    for (ObjectInfo player : this.players) {
                        if (player.getObjectId().equals(playerUnderPressureId)) {
                            ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
                            playerUnderPressure = player;
                            break;
                        }
                    }
                }

                boolean endUnderPressure = false;

                if (ballPossessionInformation.isSomeoneInBallPossession()) { // If there is a player in possession of the ball
                    ObjectInfo playerInfo = new ObjectInfo(ballPossessionInformation.getPlayerId(), ballPossessionInformation.getTeamId());
                    outputList.add(PressingStateStreamElement.generatePressingStateStreamElement(matchId, pressingTs, playerInfo, pressingIndex));

                    if (activeUnderPressure && !ballPossessionInformation.getPlayerId().equals(playerUnderPressureId)) { // If there is an active underPressure but the player under pressure is not the player in ball possession
                        // --> END UNDERPRESSURE
                        endUnderPressure = true;
                        // note that in this case the pressingIndex which is emitted in the last element for the underPressureEvent is calculated for the player which is now in ball possession and can thus be higher than the threshold
                    } else {
                        if (pressingIndex >= this.minPressingIndexForUnderPressure) { // If the current pressing index is higher than the threshold
                            if (activeUnderPressure) { // If there is an active under pressure event
                                // --> CONTINUE UNDERPRESSURE
                                long underPressureEventIdentifierCounterValue = this.underPressureEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                                outputList.add(UnderPressureEventStreamElement.generateUnderPressureEventStreamElement(matchId, pressingTs, playerUnderPressure, pressingIndex, NonAtomicEventPhase.ACTIVE, underPressureEventIdentifierCounterValue));
                            } else { // If there is no active under pressure event yet
                                // --> START UNDERPRESSURE
                                for (ObjectInfo player : this.players) {
                                    if (player.getObjectId().equals(ballPossessionInformation.getPlayerId())) {
                                        ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
                                        playerUnderPressure = player;
                                        break;
                                    }
                                }

                                this.playerUnderPressureStore.put(matchId, Schema.STATIC_INNER_KEY, playerUnderPressure.getObjectId());
                                this.underPressureEventIdentifierCounterStore.increase(matchId, Schema.STATIC_INNER_KEY, 1L);
                                long underPressureEventIdentifierCounterValue = this.underPressureEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                                outputList.add(UnderPressureEventStreamElement.generateUnderPressureEventStreamElement(matchId, pressingTs, playerUnderPressure, pressingIndex, NonAtomicEventPhase.START, underPressureEventIdentifierCounterValue));
                            }
                        } else if (activeUnderPressure) { // If there is an active under pressure event but the current pressing index is lower than the threshold
                            // --> END UNDERPRESSURE
                            endUnderPressure = true;
                        }
                    }
                } else { // If the is no player in possession of the ball
                    outputList.add(PressingStateStreamElement.generatePressingStateStreamElement(matchId, pressingTs, null, 0.0d));

                    if (activeUnderPressure) { // If there is an active under pressure event but there is no player in possession of the ball
                        // --> END UNDERPRESSURE
                        endUnderPressure = true;
                    }
                }

                if (endUnderPressure) {
                    this.playerUnderPressureStore.put(matchId, Schema.STATIC_INNER_KEY, "null");
                    long underPressureEventIdentifierCounterValue = this.underPressureEventIdentifierCounterStore.getLong(matchId, Schema.STATIC_INNER_KEY);
                    outputList.add(UnderPressureEventStreamElement.generateUnderPressureEventStreamElement(matchId, pressingTs, playerUnderPressure, pressingIndex, NonAtomicEventPhase.END, underPressureEventIdentifierCounterValue));
                }
            }
        } catch (SingleValueStore.SingleValueStoreException | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement | StoreBallPossessionInformationModule.BallPossessionInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }
}
