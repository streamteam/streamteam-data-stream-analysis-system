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

/* Copied from https://github.com/apache/samza/blob/0.13.1/samza-core/src/main/java/org/apache/samza/coordinator/stream/CoordinatorStreamSystemConsumer.java (published under the Apache License)
 * Changes:
 * - Highlighted modifications in bootstrap method
 * - Automatic code reformatting by IntelliJ
 */

package org.apache.samza.coordinator.stream;

import ch.unibas.dmi.dbis.streamTeam.samzaExtensions.KafkaMessageWithLogAppendTimestamp;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.*;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A wrapper around a SystemConsumer that provides helpful methods for dealing
 * with the coordinator stream.
 */
public class CoordinatorStreamSystemConsumer {
    private static final Logger log = LoggerFactory.getLogger(CoordinatorStreamSystemConsumer.class);

    private final Serde<List<?>> keySerde;
    private final Serde<Map<String, Object>> messageSerde;
    private final SystemStreamPartition coordinatorSystemStreamPartition;
    private final SystemConsumer systemConsumer;
    private final SystemAdmin systemAdmin;
    private final Map<String, String> configMap;
    private volatile boolean isStarted;
    private volatile boolean isBootstrapped;
    private final Object bootstrapLock = new Object();
    private volatile Set<CoordinatorStreamMessage> bootstrappedStreamSet = Collections.emptySet();

    public CoordinatorStreamSystemConsumer(SystemStream coordinatorSystemStream, SystemConsumer systemConsumer, SystemAdmin systemAdmin, Serde<List<?>> keySerde, Serde<Map<String, Object>> messageSerde) {
        this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
        this.systemConsumer = systemConsumer;
        this.systemAdmin = systemAdmin;
        this.configMap = new HashMap();
        this.isBootstrapped = false;
        this.keySerde = keySerde;
        this.messageSerde = messageSerde;
    }

    public CoordinatorStreamSystemConsumer(SystemStream coordinatorSystemStream, SystemConsumer systemConsumer, SystemAdmin systemAdmin) {
        this(coordinatorSystemStream, systemConsumer, systemAdmin, new JsonSerde<List<?>>(), new JsonSerde<Map<String, Object>>());
    }

    /**
     * Retrieves the oldest offset in the coordinator stream, and registers the
     * coordinator stream with the SystemConsumer using the earliest offset.
     */
    public void register() {
        if (this.isStarted) {
            log.info("Coordinator stream partition {} has already been registered. Skipping.", this.coordinatorSystemStreamPartition);
            return;
        }
        log.debug("Attempting to register: {}", this.coordinatorSystemStreamPartition);
        Set<String> streamNames = new HashSet<String>();
        String streamName = this.coordinatorSystemStreamPartition.getStream();
        streamNames.add(streamName);
        Map<String, SystemStreamMetadata> systemStreamMetadataMap = this.systemAdmin.getSystemStreamMetadata(streamNames);
        log.info(String.format("Got metadata %s", systemStreamMetadataMap.toString()));

        if (systemStreamMetadataMap == null) {
            throw new SamzaException("Received a null systemStreamMetadataMap from the systemAdmin. This is illegal.");
        }

        SystemStreamMetadata systemStreamMetadata = systemStreamMetadataMap.get(streamName);

        if (systemStreamMetadata == null) {
            throw new SamzaException("Expected " + streamName + " to be in system stream metadata.");
        }

        SystemStreamPartitionMetadata systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata().get(this.coordinatorSystemStreamPartition.getPartition());

        if (systemStreamPartitionMetadata == null) {
            throw new SamzaException("Expected metadata for " + this.coordinatorSystemStreamPartition + " to exist.");
        }

        String startingOffset = systemStreamPartitionMetadata.getOldestOffset();
        log.debug("Registering {} with offset {}", this.coordinatorSystemStreamPartition, startingOffset);
        this.systemConsumer.register(this.coordinatorSystemStreamPartition, startingOffset);
    }

    /**
     * Starts the underlying SystemConsumer.
     */
    public void start() {
        if (this.isStarted) {
            log.info("Coordinator stream consumer already started");
            return;
        }
        log.info("Starting coordinator stream system consumer.");
        this.systemConsumer.start();
        this.isStarted = true;
    }

    /**
     * Stops the underlying SystemConsumer.
     */
    public void stop() {
        log.info("Stopping coordinator stream system consumer.");
        this.systemConsumer.stop();
        this.isStarted = false;
    }

    /**
     * Read all messages from the earliest offset, all the way to the latest.
     * Currently, this method only pays attention to config messages.
     */
    public void bootstrap() {
        synchronized (this.bootstrapLock) {
            // Make a copy so readers aren't affected while we modify the set.
            final LinkedHashSet<CoordinatorStreamMessage> bootstrappedMessages = new LinkedHashSet<>(this.bootstrappedStreamSet);

            log.info("Bootstrapping configuration from coordinator stream.");
            SystemStreamPartitionIterator iterator =
                    new SystemStreamPartitionIterator(this.systemConsumer, this.coordinatorSystemStreamPartition);

            try {
                while (iterator.hasNext()) {
                    IncomingMessageEnvelope envelope = iterator.next();
                    Object[] keyArray = this.keySerde.fromBytes((byte[]) envelope.getKey()).toArray();
                    Map<String, Object> valueMap = null;
                    if (envelope.getMessage() != null) {
                        // === START MODIFICATION FOR STREAMTEAM PROJECT ===
                        if (envelope.getMessage() instanceof KafkaMessageWithLogAppendTimestamp) {
                            KafkaMessageWithLogAppendTimestamp kafkaMessageWithLogAppendTimestamp = (KafkaMessageWithLogAppendTimestamp) envelope.getMessage();
                            valueMap = this.messageSerde.fromBytes((byte[]) kafkaMessageWithLogAppendTimestamp.getMessage());
                        } else {
                            valueMap = this.messageSerde.fromBytes((byte[]) envelope.getMessage());
                        }
                        // === END MODIFICATION FOR STREAMTEAM PROJECT ===
                    }
                    CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, valueMap);
                    log.debug("Received coordinator stream message: {}", coordinatorStreamMessage);
                    // Remove any existing entry. Set.add() does not add if the element already exists.
                    if (bootstrappedMessages.remove(coordinatorStreamMessage)) {
                        log.debug("Removed duplicate message: {}", coordinatorStreamMessage);
                    }
                    bootstrappedMessages.add(coordinatorStreamMessage);
                    if (SetConfig.TYPE.equals(coordinatorStreamMessage.getType())) {
                        String configKey = coordinatorStreamMessage.getKey();
                        if (coordinatorStreamMessage.isDelete()) {
                            this.configMap.remove(configKey);
                        } else {
                            String configValue = new SetConfig(coordinatorStreamMessage).getConfigValue();
                            this.configMap.put(configKey, configValue);
                        }
                    }
                }

                this.bootstrappedStreamSet = Collections.unmodifiableSet(bootstrappedMessages);
                log.debug("Bootstrapped configuration: {}", this.configMap);
                this.isBootstrapped = true;
            } catch (Exception e) {
                throw new SamzaException(e);
            }
        }
    }

    public Set<CoordinatorStreamMessage> getBoostrappedStream() {
        log.info("Returning the bootstrapped data from the stream");
        if (!this.isBootstrapped)
            bootstrap();
        return this.bootstrappedStreamSet;
    }

    public Set<CoordinatorStreamMessage> getBootstrappedStream(String type) {
        log.debug("Bootstrapping coordinator stream for messages of type {}", type);
        bootstrap();
        LinkedHashSet<CoordinatorStreamMessage> bootstrappedStream = new LinkedHashSet<CoordinatorStreamMessage>();
        for (CoordinatorStreamMessage coordinatorStreamMessage : this.bootstrappedStreamSet) {
            log.trace("Considering message: {}", coordinatorStreamMessage);
            if (type.equalsIgnoreCase(coordinatorStreamMessage.getType())) {
                log.trace("Adding message: {}", coordinatorStreamMessage);
                bootstrappedStream.add(coordinatorStreamMessage);
            }
        }
        return bootstrappedStream;
    }

    /**
     * @return The bootstrapped configuration that's been read after bootstrap has
     * been invoked.
     */
    public Config getConfig() {
        if (this.isBootstrapped) {
            return new MapConfig(this.configMap);
        } else {
            throw new SamzaException("Must call bootstrap before retrieving config.");
        }
    }

    /**
     * Gets an iterator on the coordinator stream, starting from the starting offset the consumer was registered with.
     *
     * @return an iterator on the coordinator stream pointing to the starting offset the consumer was registered with.
     */
    public SystemStreamPartitionIterator getStartIterator() {
        return new SystemStreamPartitionIterator(this.systemConsumer, this.coordinatorSystemStreamPartition);
    }

    /**
     * returns all unread messages after an iterator on the stream
     *
     * @param iterator the iterator pointing to an offset in the coordinator stream. All unread messages after this iterator are returned
     * @return a set of unread messages after a given iterator
     */
    public Set<CoordinatorStreamMessage> getUnreadMessages(SystemStreamPartitionIterator iterator) {
        return getUnreadMessages(iterator, null);
    }

    /**
     * returns all unread messages of a specific type, after an iterator on the stream
     *
     * @param iterator the iterator pointing to an offset in the coordinator stream. All unread messages after this iterator are returned
     * @param type     the type of the messages to be returned
     * @return a set of unread messages of a given type, after a given iterator
     */
    public Set<CoordinatorStreamMessage> getUnreadMessages(SystemStreamPartitionIterator iterator, String type) {
        LinkedHashSet<CoordinatorStreamMessage> messages = new LinkedHashSet<CoordinatorStreamMessage>();
        while (iterator.hasNext()) {
            IncomingMessageEnvelope envelope = iterator.next();
            Object[] keyArray = this.keySerde.fromBytes((byte[]) envelope.getKey()).toArray();
            Map<String, Object> valueMap = null;
            if (envelope.getMessage() != null) {
                valueMap = this.messageSerde.fromBytes((byte[]) envelope.getMessage());
            }
            CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, valueMap);
            if (type == null || type.equals(coordinatorStreamMessage.getType())) {
                messages.add(coordinatorStreamMessage);
            }
        }
        return messages;
    }

    /**
     * Checks whether or not there are any messages after a given iterator on the coordinator stream
     *
     * @param iterator The iterator to check if there are any new messages after this point
     * @return True if there are new messages after the iterator, false otherwise
     */
    public boolean hasNewMessages(SystemStreamPartitionIterator iterator) {
        if (iterator == null) {
            return false;
        }
        return iterator.hasNext();
    }

}