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
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.TeamAreaStateStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.SingleElementProcessorInterface;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.ObjectInfoFactoryAndModifier;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;
import org.kynosarges.tektosyne.geometry.GeoUtils;
import org.kynosarges.tektosyne.geometry.PointD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Module for generating teamAreaState stream elements.
 * Assumes that the positionStore and the fieldObjectStateTsStore is filled by a StoreModule.
 * Further assumes that the input is filtered beforehand and contains only fieldObjectState stream elements for the players.
 * Generates a teamAreaState stream element whenever a fieldObjectState stream element updates an area defined by the players of a team.
 */
public class TeamAreaModule implements SingleElementProcessorInterface {

    /**
     * Slf4j logger
     */
    private final static Logger logger = LoggerFactory.getLogger(TeamAreaModule.class);

    /**
     * SingleValueStore that contains the position of the latest fieldObjectState stream element of every player (filled by a StoreModule)
     */
    private final SingleValueStore<Geometry.Vector> positionStore;

    /**
     * SingleValueStore that contains generation timestamp of the latest fieldObjectState stream element of every player (filled by a StoreModule)
     */
    private final SingleValueStore<Long> fieldObjectStateTsStore;

    /**
     * SingleValueStore that contains last minimum bounding box surface emitted in the last teamAreaState stream (for each team)
     */
    private final SingleValueStore<Double> mbrSurfaceStore;

    /**
     * SingleValueStore that contains last planar convex hull surface emitted in the last teamAreaState stream (for each team)
     */
    private final SingleValueStore<Double> pchSurfaceStore;

    /**
     * A list of all players
     */
    private final List<ObjectInfo> players;

    /**
     * TeamAreaModule constructor.
     *
     * @param players                 A list of all players
     * @param positionStore           SingleValueStore that contains the position of the latest fieldObjectState stream element of every player (filled by a StoreModule)
     * @param fieldObjectStateTsStore SingleValueStore that contains generation timestamp of the latest fieldObjectState stream element of every player (filled by a StoreModule)
     * @param mbrSurfaceStore         SingleValueStore that contains last minimum bounding box surface emitted in the last teamAreaState stream (for each team)
     * @param pchSurfaceStore         SingleValueStore that contains last planar convex hull surface emitted in the last teamAreaState stream (for each team)
     */
    public TeamAreaModule(List<ObjectInfo> players, SingleValueStore<Geometry.Vector> positionStore, SingleValueStore<Long> fieldObjectStateTsStore, SingleValueStore<Double> mbrSurfaceStore, SingleValueStore<Double> pchSurfaceStore) {
        this.players = players;
        this.positionStore = positionStore;
        this.fieldObjectStateTsStore = fieldObjectStateTsStore;
        this.mbrSurfaceStore = mbrSurfaceStore;
        this.pchSurfaceStore = pchSurfaceStore;
    }

    /**
     * Generates a teamAreaState stream element whenever a fieldObjectState stream element updates an area defined by the players of a team.
     * Assumes to process only fieldObjectState stream elements for players.
     *
     * @param inputDataStreamElement fieldObjectState stream element for a player
     * @return teamAreaState stream element if an area defined by the players of a team is updated
     */
    @Override
    public List<AbstractImmutableDataStreamElement> processElement(AbstractImmutableDataStreamElement inputDataStreamElement) {
        List<AbstractImmutableDataStreamElement> outputList = new LinkedList<>();

        FieldObjectStateStreamElement fieldObjectStateStreamElement = (FieldObjectStateStreamElement) inputDataStreamElement;

        String matchId = fieldObjectStateStreamElement.getKey();
        long ts = fieldObjectStateStreamElement.getGenerationTimestamp();

        try {
            String teamId = fieldObjectStateStreamElement.getTeamId();

            long generationTimestamp = 0;
            List<ObjectInfo> playersOfTheTeam = new LinkedList<>();
            for (ObjectInfo player : this.players) {
                if (player.getGroupId().equals(teamId)) {
                    ObjectInfoFactoryAndModifier.updateObjectInfoPositionWithSingleValueStore(player, matchId, this.positionStore);
                    playersOfTheTeam.add(player);

                    long lastTsForPlayer = this.fieldObjectStateTsStore.getLong(matchId, player.getObjectId());
                    if (lastTsForPlayer > generationTimestamp) {
                        generationTimestamp = lastTsForPlayer; // Generation timestamp of the teamAreaState stream element = Maximum generation timestamp of all fieldObjectState stream elements of this team received so far
                    }
                }
            }

            double mbrSurface = calculateMinimumBoundingRectangleSurface(playersOfTheTeam);
            double pchSurface = calculatePlanarConvexHullSurface(playersOfTheTeam);

            Double lastMbrSurface = this.mbrSurfaceStore.get(matchId, teamId);
            Double lastPchSurface = this.pchSurfaceStore.get(matchId, teamId);

            boolean changed = false;
            if (lastMbrSurface == null || Math.abs(lastMbrSurface - mbrSurface) > 0.00001) {
                this.mbrSurfaceStore.put(matchId, teamId, mbrSurface);
                changed = true;
            }
            if (lastPchSurface == null || Math.abs(lastPchSurface - pchSurface) > 0.00001) {
                this.pchSurfaceStore.put(matchId, teamId, pchSurface);
                changed = true;
            }

            if (changed) {
                outputList.add(TeamAreaStateStreamElement.generateTeamAreaStateStreamElement(matchId, generationTimestamp, teamId, playersOfTheTeam, mbrSurface, pchSurface));
            }

        } catch (SingleValueStore.SingleValueStoreException | AbstractImmutableDataStreamElement.CannotRetrieveInformationException | AbstractImmutableDataStreamElement.CannotGenerateDataStreamElement | ObjectInfoFactoryAndModifier.CannotCreateOrUpdateObjectInfoException e) {
            logger.error("Caught exception during processing element: {}", inputDataStreamElement, e);
        }

        return outputList;
    }

    /**
     * Calculates the surface of the minimum bounding rectangle around the players.
     *
     * @param players Players of the team
     * @return Surface of the minimum bounding rectangle around the players
     */
    private double calculateMinimumBoundingRectangleSurface(List<ObjectInfo> players) {
        if (players.size() == 0) {
            return 0.0;
        } else {
            double minX = Double.MAX_VALUE;
            double maxX = Double.MIN_VALUE;
            double minY = Double.MAX_VALUE;
            double maxY = Double.MIN_VALUE;

            for (ObjectInfo player : players) {
                if (player.getPosition().x < minX) {
                    minX = player.getPosition().x;
                }
                if (player.getPosition().x > maxX) {
                    maxX = player.getPosition().x;
                }
                if (player.getPosition().y < minY) {
                    minY = player.getPosition().y;
                }
                if (player.getPosition().y > maxY) {
                    maxY = player.getPosition().y;
                }
            }

            return (maxX - minX) * (maxY - minY);
        }
    }

    /**
     * Calculates the surface of the planar convex hull around the players.
     *
     * @param players Players of the team
     * @return Surface of the planar convex hull around the players
     */
    private double calculatePlanarConvexHullSurface(List<ObjectInfo> players) {
        if (players.size() == 0) {
            return 0.0;
        } else {
            // Create array of planar player positions in correct format for the convexHull method of the tektosyne library
            PointD[] planarPlayerPositions = new PointD[players.size()];
            for (int i = 0; i < players.size(); ++i) {
                planarPlayerPositions[i] = new PointD(players.get(i).getPosition().x, players.get(i).getPosition().y);
            }

            // Calculate planar convex hull
            PointD[] convexHullPoints = GeoUtils.convexHull(planarPlayerPositions); // resulting point list is sorted clock-wise

            // Generate list of convex hull points as Vectors sorted counterclock-wise
            List<Geometry.Vector> counterclockwiseSortedConvexHullVectors = new ArrayList<>(convexHullPoints.length);
            for (int i = convexHullPoints.length - 1; i >= 0; --i) {
                counterclockwiseSortedConvexHullVectors.add(new Geometry.Vector(convexHullPoints[i].x, convexHullPoints[i].y, 0.0));
            }

            // Calculate the surface of the planar convex hull using surveyor's formula
            return calculatePolygonSurface(counterclockwiseSortedConvexHullVectors);
        }
    }

    /**
     * Calculates the surface of a simple polygon.
     * See paper "The Surveyor's Area Formula" for more details.
     *
     * @param polygonPoints Counterclock-wise sorted list of polygon points
     * @return Surface of the polygon
     */
    private double calculatePolygonSurface(List<Geometry.Vector> polygonPoints) {
        double surface = 0.0;
        for (int i = 0; i < polygonPoints.size() - 1; ++i) {
            surface += (polygonPoints.get(i).x * polygonPoints.get(i + 1).y) - (polygonPoints.get(i + 1).x * polygonPoints.get(i).y); //determinant for points i and i+1
        }
        surface += (polygonPoints.get(polygonPoints.size() - 1).x * polygonPoints.get(0).y) - (polygonPoints.get(0).x * polygonPoints.get(polygonPoints.size() - 1).y); // determinant for last and first point
        surface *= 0.5;
        return surface;
    }
}
