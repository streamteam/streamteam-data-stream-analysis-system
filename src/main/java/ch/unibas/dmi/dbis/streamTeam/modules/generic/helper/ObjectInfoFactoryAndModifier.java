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

package ch.unibas.dmi.dbis.streamTeam.modules.generic.helper;

import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;

/**
 * Factory for generating ObjectInfos.
 */
public class ObjectInfoFactoryAndModifier {

    /**
     * Creates an ObjectInfo with position from a single value store.
     *
     * @param key           Key for the single value store access
     * @param objectId      Identifier of the object (BALL, A* or B*)
     * @param teamId        Identifier of the team the object belongs to
     * @param positionStore SingleValueStore containing the position
     * @return ObjectInfo
     * @throws CannotCreateOrUpdateObjectInfoException Thrown if the position cannot be read from the single value store
     */
    public static ObjectInfo createObjectInfoWithPositionFromSingleValueStore(String key, String objectId, String teamId, SingleValueStore<Geometry.Vector> positionStore) throws CannotCreateOrUpdateObjectInfoException {
        Geometry.Vector position = positionStore.get(key, objectId);
        if (position != null) {
            return new ObjectInfo(objectId, teamId, position);
        } else {
            throw new CannotCreateOrUpdateObjectInfoException("Cannot retrieve player position info: The position SingleValueStore for object " + objectId + " is not filled sufficiently.");
        }
    }

    /**
     * Creates an ObjectInfo with position from a history store.
     *
     * @param key           Key for the history store access
     * @param objectId      Identifier of the object (BALL, A* or B*)
     * @param teamId        Identifier of the team the object belongs to
     * @param positionStore SingleValueStore containing the position
     * @return ObjectInfo
     * @throws CannotCreateOrUpdateObjectInfoException Thrown if the position cannot be read from the history store
     */
    public static ObjectInfo createObjectInfoWithPositionFromHistoryStore(String key, String objectId, String teamId, HistoryStore<Geometry.Vector> positionStore) throws CannotCreateOrUpdateObjectInfoException {
        Geometry.Vector position = positionStore.getLatest(key, objectId);
        if (position != null) {
            return new ObjectInfo(objectId, teamId, position);
        } else {
            throw new CannotCreateOrUpdateObjectInfoException("Cannot retrieve player position info: The position HistoryStore for object " + objectId + " is not filled sufficiently.");
        }
    }

    /**
     * Updates the position of an ObjectInfo with position from a single value store.
     *
     * @param objectInfo    ObjectInfo which will be updated
     * @param key           Key for the single value store access
     * @param positionStore SingleValueStore containing the position
     * @throws CannotCreateOrUpdateObjectInfoException Thrown if the positions cannot be read from the single value store
     */
    public static void updateObjectInfoPositionWithSingleValueStore(ObjectInfo objectInfo, String key, SingleValueStore<Geometry.Vector> positionStore) throws CannotCreateOrUpdateObjectInfoException {
        Geometry.Vector position = positionStore.get(key, objectInfo.getObjectId());
        if (position != null) {
            objectInfo.setPosition(position);
        } else {
            throw new CannotCreateOrUpdateObjectInfoException("Cannot retrieve player position info: The position SingleValueStore for object " + objectInfo.getObjectId() + " is not filled sufficiently.");
        }
    }

    /**
     * Updates the position of an ObjectInfo with position from a history store.
     *
     * @param objectInfo    ObjectInfo which will be updated
     * @param key           Key for the history store access
     * @param positionStore HistoryStore containing the position
     * @throws CannotCreateOrUpdateObjectInfoException Thrown if the positions cannot be read from the history store
     */
    public static void updateObjectInfoPositionWithHistoryStore(ObjectInfo objectInfo, String key, HistoryStore<Geometry.Vector> positionStore) throws CannotCreateOrUpdateObjectInfoException {
        Geometry.Vector position = positionStore.getLatest(key, objectInfo.getObjectId());
        if (position != null) {
            objectInfo.setPosition(position);
        } else {
            throw new CannotCreateOrUpdateObjectInfoException("Cannot retrieve player position info: The position HistoryStore for object " + objectInfo.getObjectId() + " is not filled sufficiently.");
        }
    }

    /**
     * Updates the velocity of an ObjectInfo with velocity from single value stores.
     *
     * @param objectInfo       ObjectInfo which will be updated
     * @param key              Key for the single value store access
     * @param velocityXStore   SingleValueStore for the x component of the velocity
     * @param velocityYStore   SingleValueStore for the y component of the velocity
     * @param velocityZStore   SingleValueStore for the z component of the velocity
     * @param velocityAbsStore SingleValueStore for the absolute velocity
     * @throws CannotCreateOrUpdateObjectInfoException Thrown if the velocity cannot be read from the single value stores
     */
    public static void updateObjectInfoVelocityWithSingleValueStores(ObjectInfo objectInfo, String key, SingleValueStore<Double> velocityXStore, SingleValueStore<Double> velocityYStore, SingleValueStore<Double> velocityZStore, SingleValueStore<Double> velocityAbsStore) throws CannotCreateOrUpdateObjectInfoException {
        Double vx = velocityXStore.get(key, objectInfo.getObjectId());
        Double vy = velocityYStore.get(key, objectInfo.getObjectId());
        Double vz = velocityZStore.get(key, objectInfo.getObjectId());
        Double vabs = velocityAbsStore.get(key, objectInfo.getObjectId());
        if (vx != null && vy != null && vz != null && vabs != null) {
            objectInfo.setVelocity(new Geometry.Vector(vx, vy, vz));
            objectInfo.setAbsoluteVelocity(vabs);
        } else {
            throw new CannotCreateOrUpdateObjectInfoException("Cannot retrieve player velocity info: The vx, vy, vz or vabs SingleValueStore for object " + objectInfo.getObjectId() + " is not filled sufficiently.");
        }
    }

    /**
     * Updates the velocity of an ObjectInfo with velocity from history stores.
     *
     * @param objectInfo       ObjectInfo which will be updated
     * @param key              Key for the single value store access
     * @param velocityXStore   HistoryStore for the x component of the velocity
     * @param velocityYStore   HistoryStore for the y component of the velocity
     * @param velocityZStore   HistoryStore for the z component of the velocity
     * @param velocityAbsStore HistoryStore for the absolute velocity
     * @throws CannotCreateOrUpdateObjectInfoException Thrown if the velocity cannot be read from the history stores
     */
    public static void updateObjectInfoVelocityWithHistoryStores(ObjectInfo objectInfo, String key, HistoryStore<Double> velocityXStore, HistoryStore<Double> velocityYStore, HistoryStore<Double> velocityZStore, HistoryStore<Double> velocityAbsStore) throws CannotCreateOrUpdateObjectInfoException {
        Double vx = velocityXStore.getLatest(key, objectInfo.getObjectId());
        Double vy = velocityYStore.getLatest(key, objectInfo.getObjectId());
        Double vz = velocityZStore.getLatest(key, objectInfo.getObjectId());
        Double vabs = velocityAbsStore.getLatest(key, objectInfo.getObjectId());
        if (vx != null && vy != null && vz != null && vabs != null) {
            objectInfo.setVelocity(new Geometry.Vector(vx, vy, vz));
            objectInfo.setAbsoluteVelocity(vabs);
        } else {
            throw new CannotCreateOrUpdateObjectInfoException("Cannot retrieve player velocity info: The vx, vy, vz or vabs HistoryStore for object " + objectInfo.getObjectId() + " is not filled sufficiently.");
        }
    }

    /**
     * Creates an ObjectInfo (without position) by means of a player definition string.
     *
     * @param playerDefinition Player definition string
     * @return ObjectInfo
     */
    public static ObjectInfo createObjectInfoFromPlayerDefinitionString(String playerDefinition) {
        String definitionInner = playerDefinition.substring(1, playerDefinition.length() - 1); // remove surrounding brackets
        String[] definitionParts = definitionInner.split(":");
        String id = definitionParts[0];
        String teamId = definitionParts[1];
        return new ObjectInfo(id, teamId);
    }

    /**
     * Creates an ObjectInfo (without position) by means of a ball definition string.
     *
     * @param ballDefinition Ball definition string
     * @return ObjectInfo
     */
    public static ObjectInfo createObjectInfoFromBallDefinitionString(String ballDefinition) {
        return new ObjectInfo(ballDefinition, ballDefinition);
    }

    /**
     * Indicates that the ObjectInfo cannot be created or updated.
     */
    public static class CannotCreateOrUpdateObjectInfoException extends Exception {

        /**
         * CannotGenerateObjectInfoException constructor.
         *
         * @param msg Message that explains the problem
         */
        public CannotCreateOrUpdateObjectInfoException(String msg) {
            super(msg);
        }
    }
}
