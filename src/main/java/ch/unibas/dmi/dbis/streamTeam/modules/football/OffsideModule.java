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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.OffsideLineStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
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
 * Module for generating offsideLineState stream elements.
 * Assumes that the the positionStore and the leftTeamIdStore are filled by StoreModules and that the ballPossessionInformationStore is filled by the StoreBallPossessionInformationModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the players.
 * Generates an offsideLineState stream element for every fieldObjectState stream element of the player in ball possession.
 */
public class OffsideModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(OffsideModule.class);

    /**
     * SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     */
    private final SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore;

    /**
     * SingleValueStore that contains the position of the latest fieldObjectState stream element of every player (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains the leftTeamId of the latest kickoffEvent steam element (filled by a StoreModule)
     */
    private final SingleValueStore<String> leftTeamIdStore;

    /**
     * SingleValueStore that contains information if the last generated offsideLineState stream element has been a null offsideLineState stream element
     */
    private final SingleValueStore<Boolean> lastOffsideLineWasNullStore;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * OffsideModule constructor.
     *
     * @param players                        A list of all players
     * @param ballPossessionInformationStore SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element (filled by StoreBallPossessionInformationModule)
     * @param positionStore                  SingleValueStore that contains the position of the latest fieldObjectState stream element of every player (filled by a StoreModule)
     * @param leftTeamIdStore                SingleValueStore that contains the leftTeamId of the latest teamSides steam element (filled by a StoreModule)
     * @param lastOffsideLineWasNullStore    SingleValueStore that contains information if the last generated offsideLineState stream element has been a null offsideLineState stream element (filled by a StoreModule)
     */
    public OffsideModule(List<ObjectInfo> players, SingleValueStore<StoreBallPossessionInformationModule.BallPossessionInformation> ballPossessionInformationStore, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<String> leftTeamIdStore, SingleValueStore<Boolean> lastOffsideLineWasNullStore) {
        this.players = players;
        this.ballPossessionInformationStore = ballPossessionInformationStore;
        this.positionStore = positionStore;
        this.leftTeamIdStore = leftTeamIdStore;
        this.lastOffsideLineWasNullStore = lastOffsideLineWasNullStore;
    }

    /**
     * Generates an offsideLineState stream element for every fieldObjectState stream element of the player in ball possession.
     * Assumes to process only fieldObjectState stream elements for players.
     *
     * @param inputDataStreamElement fieldObjectState stream element for a player
     * @return offsideLineState stream element if there is a player in possession of the ball
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        String matchId = fieldObjectStateStreamElement.getKey();
        long ts = fieldObjectStateStreamElement.getGenerationTimestamp();

        try {
            StoreBallPossessionInformationModule.BallPossessionInformation ballPossessionInformation = this.ballPossessionInformationStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (ballPossessionInformation == null) {
                ballPossessionInformation = new StoreBallPossessionInformationModule.BallPossessionInformation(false, null, null);
            }

            if (!ballPossessionInformation.isSomeoneInBallPossession()) { // if no player is in possession of the ball
                if (!this.lastOffsideLineWasNullStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                    outputList.add(OffsideLineStateStreamElement.generateOffsideLineStateStreamElement(matchId, ts, null, null, 0.0));
                    this.lastOffsideLineWasNullStore.put(matchId, Schema.STATIC_INNER_KEY, true);
                }
            } else {
                if (fieldObjectStateStreamElement.getObjectId().equals(ballPossessionInformation.getPlayerId())) { // if the fieldObjectState stream element is for the player in ball possession
                    OffsideLineResult offsideLineResult = calculateOffsideLine(fieldObjectStateStreamElement);
                    ObjectInfo playerInBallPossession = new ObjectInfo(ballPossessionInformation.getPlayerId(), fieldObjectStateStreamElement.getTeamId(), fieldObjectStateStreamElement.getPosition());
                    outputList.add(OffsideLineStateStreamElement.generateOffsideLineStateStreamElement(matchId, ts, playerInBallPossession, offsideLineResult.playersInOffsidePosition, offsideLineResult.offsideLineX));

                    if (this.lastOffsideLineWasNullStore.getBoolean(matchId, Schema.STATIC_INNER_KEY)) {
                        this.lastOffsideLineWasNullStore.put(matchId, Schema.STATIC_INNER_KEY, false);
                    }
                }
            }
        } catch (OffsideException | SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement | StoreBallPossessionInformationModule.BallPossessionInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Calculates the offside line and the players in offside position.
     *
     * @param fieldObjectStateStreamElement fieldObjectState stream element for the player in ball possession
     * @return offside line result
     * @throws OffsideException Thrown if the offside line or the players in offside position could not be calculated
     */
    private OffsideLineResult calculateOffsideLine(FieldObjectStateStreamElement fieldObjectStateStreamElement) throws OffsideException {
        try {
            String matchId = fieldObjectStateStreamElement.getKey();
            String leftTeamId = this.leftTeamIdStore.get(matchId, Schema.STATIC_INNER_KEY);
            if (leftTeamId != null) {
                String teamId = fieldObjectStateStreamElement.getTeamId();
                double ballPossessionPlayerX = fieldObjectStateStreamElement.getPosition().x;

                double offsideLineX = ballPossessionPlayerX;

                boolean playToRight = leftTeamId.equals(teamId);

                // Generate list of own team and foreign team players and get positions from single value store
                List<ObjectInfo> playersInOwnTeam = new LinkedList<>();
                List<ObjectInfo> playersInForeignTeam = new LinkedList<>();
                for (ObjectInfo player : this.players) {
                    ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
                    if (player.getGroupId().equals(teamId)) {
                        playersInOwnTeam.add(player);
                    } else {
                        playersInForeignTeam.add(player);
                    }
                }

                // Calculate the last and the second last foreign player x position in playing direction
                double lastForeignPlayerX;
                double secondLastForeignPlayerX;
                if (playToRight) {
                    lastForeignPlayerX = Double.MIN_VALUE;
                    secondLastForeignPlayerX = Double.MIN_VALUE;
                } else {
                    lastForeignPlayerX = Double.MAX_VALUE;
                    secondLastForeignPlayerX = Double.MAX_VALUE;
                }
                for (ObjectInfo foreignPlayer : playersInForeignTeam) {
                    double foreignPlayerX = foreignPlayer.getPosition().x;
                    if (playToRight) {
                        if (foreignPlayerX > lastForeignPlayerX) {
                            secondLastForeignPlayerX = lastForeignPlayerX;
                            lastForeignPlayerX = foreignPlayerX;
                        } else if (foreignPlayerX < lastForeignPlayerX && foreignPlayerX > secondLastForeignPlayerX) {
                            secondLastForeignPlayerX = foreignPlayerX;
                        }
                    } else {
                        if (foreignPlayerX < lastForeignPlayerX) {
                            secondLastForeignPlayerX = lastForeignPlayerX;
                            lastForeignPlayerX = foreignPlayerX;
                        } else if (foreignPlayerX > lastForeignPlayerX && foreignPlayerX < secondLastForeignPlayerX) {
                            secondLastForeignPlayerX = foreignPlayerX;
                        }
                    }
                }

                // Consider that a back pass is always allowed, i.e., the offside line is at least at the position of the player in ball possession
                if (playToRight) {
                    if (secondLastForeignPlayerX > offsideLineX) {
                        offsideLineX = secondLastForeignPlayerX;
                    }
                } else {
                    if (secondLastForeignPlayerX < offsideLineX) {
                        offsideLineX = secondLastForeignPlayerX;
                    }
                }

                // Generate the list of players in offside position
                List<ObjectInfo> playersInOffsidePositon = new LinkedList<>();
                for (ObjectInfo ownPlayer : playersInOwnTeam) {
                    Double ownPlayerX = ownPlayer.getPosition().x;
                    if (ownPlayerX != null) {
                        if (playToRight) {
                            if (ownPlayerX > offsideLineX) {
                                playersInOffsidePositon.add(ownPlayer);
                            }
                        } else {
                            if (ownPlayerX < offsideLineX) {
                                playersInOffsidePositon.add(ownPlayer);
                            }
                        }
                    }
                }

                return new OffsideLineResult(offsideLineX, playersInOffsidePositon);
            } else {
                throw new OffsideException("Cannot calculate offside line: The leftTeamId single value store for kickoffEvent and/or the position single value store for fieldObjectState is not filled sufficiently (by a StoreModule).");
            }
        } catch (NumberFormatException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException e) {
            throw new OffsideException("Cannot calculate offside line: " + e.toString());
        }
    }

    /**
     * Result of the offside line calculation.
     */
    private static class OffsideLineResult {

        /**
         * X position of the offside line
         */
        private final double offsideLineX;

        /**
         * List of players in offside position
         */
        private final List<ObjectInfo> playersInOffsidePosition;

        /**
         * OffsideLineResult constructor.
         *
         * @param offsideLineX             X position of the offside line
         * @param playersInOffsidePosition List of players in offside position
         */
        private OffsideLineResult(double offsideLineX, List<ObjectInfo> playersInOffsidePosition) {
            this.offsideLineX = offsideLineX;
            this.playersInOffsidePosition = playersInOffsidePosition;
        }
    }

    /**
     * Indicates that the offside related stream elements could not be generated.
     */
    public static class OffsideException extends Exception {

        /**
         * OffsideException constructor.
         *
         * @param msg Message that explains the problem
         */
        public OffsideException(String msg) {
            super(msg);
        }
    }
}
