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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.BallPossessionChangeEventStreamElement;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.Schema;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Module for extracting and storing ball possession information from ballPossessionChangeEvent stream elements.
 * Assumes that the input is filtered beforehand and contains only ballPossessionChangeEvent stream elements.
 * Does not produce output data stream elements.
 * The normal StoreModule is not sufficient since objectIdentifier[0] and groupIdentifier[0] are only available in a ballPossessionChangeEvent stream element if some player is in ball possession.
 */
public class StoreBallPossessionInformationModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(StoreBallPossessionInformationModule.class);

    /**
     * SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element
     */
    private final SingleValueStore<BallPossessionInformation> ballPossessionInformationStore;

    /**
     * StoreBallPossessionInformationModule constructor.
     *
     * @param ballPossessionInformationStore SingleValueStore that contains information extracted from the latest ballPossessionChangeEvent stream element
     */
    public StoreBallPossessionInformationModule(SingleValueStore<BallPossessionInformation> ballPossessionInformationStore) {
        this.ballPossessionInformationStore = ballPossessionInformationStore;
    }

    /**
     * Extracts and stores ball possession information from ballPossessionChangeEvent stream elements.
     * Assumes that the input is filtered beforehand and contains only ballPossessionChangeEvent stream elements.
     * Does not produce output data stream elements.
     *
     * @param inputDataStreamElement ballPossessionChangeEvent stream element for the ball
     * @return Empty list
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        BallPossessionChangeEventStreamElement ballPossessionChangeEventStreamElement = (BallPossessionChangeEventStreamElement) inputDataStreamElement;

        String matchId = ballPossessionChangeEventStreamElement.getKey();

        try {
            if (ballPossessionChangeEventStreamElement.isSomeoneInBallPossession()) {
                this.ballPossessionInformationStore.put(matchId, Schema.STATIC_INNER_KEY, new BallPossessionInformation(true, ballPossessionChangeEventStreamElement.getPlayerId(), ballPossessionChangeEventStreamElement.getTeamId()));
            } else {
                this.ballPossessionInformationStore.put(matchId, Schema.STATIC_INNER_KEY, new BallPossessionInformation(false, null, null));
            }

        } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException | BallPossessionInformationException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }
        return outputList;
    }

    /**
     * Container holding ball possession information.
     */
    public static class BallPossessionInformation implements Serializable {

        /**
         * Specifies if a player is in possession of the ball or not
         */
        private final boolean someoneInBallPossession;

        /**
         * Identifier of the player in ball possession or null if no player is in possession of the ball
         */
        private final String playerId;

        /**
         * Identifier of the team whose player is in ball possession or null if no player is in possession of the ball
         */
        private final String teamId;

        /**
         * BallPossessionInformation constructor.
         *
         * @param someoneInBallPossession Specifies if some player is in possession of the ball or not
         * @param playerId                Identifier of the player in ball possession or null if no player is in possession of the ball
         * @param teamId                  Identifier of the team whose player is in ball possession or null if no player is in possession of the ball
         * @throws BallPossessionInformationException Thrown if the ball possession information cannot be generated
         */
        public BallPossessionInformation(boolean someoneInBallPossession, String playerId, String teamId) throws BallPossessionInformationException {
            if (!someoneInBallPossession && (playerId != null || teamId != null)) {
                throw new BallPossessionInformationException("someoneInBallPossession is false but playerId and/or teamId are not null.");
            } else if (someoneInBallPossession && (playerId == null || teamId == null)) {
                throw new BallPossessionInformationException("someoneInBallPossession is true but playerId and/or teamId are null.");
            }
            this.someoneInBallPossession = someoneInBallPossession;
            this.playerId = playerId;
            this.teamId = teamId;
        }

        /**
         * Gets the information if a player is in possession of the ball or not.
         *
         * @return True if a player is in possession of the ball, false if not
         */
        public boolean isSomeoneInBallPossession() {
            return this.someoneInBallPossession;
        }

        /**
         * Gets the identifier of the player in ball possession or null if no player is in possession of the ball.
         *
         * @return Identifier of the player in ball possession or null if no player is in possession of the ball
         */
        public String getPlayerId() {
            return this.playerId;
        }

        /**
         * Gets the identifier of the team whose player is in ball possession or null if no player is in possession of the ball.
         *
         * @return Identifier of the team whose player is in ball possession or null if no player is in possession of the ball
         */
        public String getTeamId() {
            return this.teamId;
        }
    }

    /**
     * Indicates that the ball possession information could not be generated.
     */
    public static class BallPossessionInformationException extends Exception {

        /**
         * BallPossessionInformationException constructor.
         *
         * @param msg Message that explains the problem
         */
        public BallPossessionInformationException(String msg) {
            super(msg);
        }
    }
}
