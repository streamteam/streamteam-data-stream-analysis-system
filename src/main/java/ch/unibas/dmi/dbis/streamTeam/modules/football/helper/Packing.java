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

package ch.unibas.dmi.dbis.streamTeam.modules.football.helper;

import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;

import java.util.Set;

/**
 * Helper class for packing calculations.
 */
public class Packing {

    /**
     * Calculates the number of players who are nearer to the goal than the player in ball possession.
     *
     * @param playerInPossessionPosition Position of the player in ball possession
     * @param goalPosition               Position of the goal
     * @param foreignTeamPlayerPositions Position of all players in the foreign team
     * @return Number of players nearer to the goal than the player in ball possession
     */
    public static int calculateNumPlayersNearerToGoal(Geometry.Vector playerInPossessionPosition, Geometry.Vector goalPosition, Set<Geometry.Vector> foreignTeamPlayerPositions) {
        int numPlayersNearerToGoal = 0;

        double distPlayerInPossession = Geometry.distance(playerInPossessionPosition, goalPosition);

        for (Geometry.Vector foreignPlayerPos : foreignTeamPlayerPositions) {
            double distForeignPlayer = Geometry.distance(foreignPlayerPos, goalPosition);

            if (distForeignPlayer < distPlayerInPossession) {
                numPlayersNearerToGoal++;
            }
        }

        return numPlayersNearerToGoal;
    }
}
