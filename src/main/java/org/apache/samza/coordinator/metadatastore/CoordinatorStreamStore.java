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

/* Copied from https://github.com/apache/samza/blob/1.5.1/samza-core/src/main/java/org/apache/samza/coordinator/metadatastore/CoordinatorStreamStore.java (published under the Apache License)
 * Changes:
 * - Highlighted modifications in readMessagesFromCoordinatorStream method
 * - Automatic code reformatting by IntelliJ
 */

package org.apache.samza.coordinator.metadatastore;

import ch.unibas.dmi.dbis.streamTeam.samzaExtensions.KafkaMessageWithLogAppendTimestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.CoordinatorStreamKeySerde;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.*;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of the {@link MetadataStore} interface where the metadata of the samza job is stored in coordinator stream.
 * <p>
 * This class is thread safe.
 * <p>
 * It is recommended to use {@link NamespaceAwareCoordinatorStreamStore}. This will enable the single CoordinatorStreamStore connection
 * to be shared by the multiple {@link NamespaceAwareCoordinatorStreamStore} instances.
 */
public class CoordinatorStreamStore implements MetadataStore {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorStreamStore.class);
    private static final String SOURCE = "SamzaContainer";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Config config;
    private final SystemStream coordinatorSystemStream;
    private final SystemStreamPartition coordinatorSystemStreamPartition;
    private final SystemProducer systemProducer;
    private final SystemConsumer systemConsumer;
    private final SystemAdmin systemAdmin;

    // Namespaced key to the message byte array.
    private final Map<String, byte[]> messagesReadFromCoordinatorStream = new ConcurrentHashMap<>();

    private final Object bootstrapLock = new Object();
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    private SystemStreamPartitionIterator iterator;

    public CoordinatorStreamStore(Config config, MetricsRegistry metricsRegistry) {
        this.config = config;
        this.coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
        this.coordinatorSystemStreamPartition = new SystemStreamPartition(this.coordinatorSystemStream, new Partition(0));
        SystemFactory systemFactory = CoordinatorStreamUtil.getCoordinatorSystemFactory(config);
        this.systemProducer = systemFactory.getProducer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry);
        this.systemConsumer = systemFactory.getConsumer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry);
        this.systemAdmin = systemFactory.getAdmin(this.coordinatorSystemStream.getSystem(), config);
    }

    @VisibleForTesting
    protected CoordinatorStreamStore(Config config, SystemProducer systemProducer, SystemConsumer systemConsumer, SystemAdmin systemAdmin) {
        this.config = config;
        this.systemConsumer = systemConsumer;
        this.systemProducer = systemProducer;
        this.systemAdmin = systemAdmin;
        this.coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
        this.coordinatorSystemStreamPartition = new SystemStreamPartition(this.coordinatorSystemStream, new Partition(0));
    }

    @Override
    public void init() {
        if (this.isInitialized.compareAndSet(false, true)) {
            LOG.info("Starting the coordinator stream system consumer with config: {}.", this.config);
            registerConsumer();
            this.systemConsumer.start();
            this.systemProducer.register(SOURCE);
            this.systemProducer.start();
            this.iterator = new SystemStreamPartitionIterator(this.systemConsumer, this.coordinatorSystemStreamPartition);
            readMessagesFromCoordinatorStream();
        } else {
            LOG.info("Store had already been initialized. Skipping.", this.coordinatorSystemStreamPartition);
        }
    }

    @Override
    public byte[] get(String namespacedKey) {
        readMessagesFromCoordinatorStream();
        return this.messagesReadFromCoordinatorStream.get(namespacedKey);
    }

    @Override
    public void put(String namespacedKey, byte[] value) {
        // 1. Store the namespace and key into correct fields of the CoordinatorStreamKey and convert the key to bytes.
        CoordinatorMessageKey coordinatorMessageKey = deserializeCoordinatorMessageKeyFromJson(namespacedKey);
        CoordinatorStreamKeySerde keySerde = new CoordinatorStreamKeySerde(coordinatorMessageKey.getNamespace());
        byte[] keyBytes = keySerde.toBytes(coordinatorMessageKey.getKey());

        // 2. Set the key, message in correct fields of {@link OutgoingMessageEnvelope} and publish it to the coordinator stream.
        OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(this.coordinatorSystemStream, 0, keyBytes, value);
        this.systemProducer.send(SOURCE, envelope);
    }

    @Override
    public void delete(String namespacedKey) {
        // Since kafka doesn't support individual message deletion, store value as null for a namespacedKey to delete.
        put(namespacedKey, null);
    }

    @Override
    public Map<String, byte[]> all() {
        readMessagesFromCoordinatorStream();
        return Collections.unmodifiableMap(this.messagesReadFromCoordinatorStream);
    }

    private void readMessagesFromCoordinatorStream() {
        synchronized (this.bootstrapLock) {
            while (this.iterator.hasNext()) {
                IncomingMessageEnvelope envelope = this.iterator.next();
                byte[] keyAsBytes = (byte[]) envelope.getKey();
                Serde<List<?>> serde = new JsonSerde<>();
                Object[] keyArray = serde.fromBytes(keyAsBytes).toArray();
                CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, new HashMap<>());
                String namespacedKey = serializeCoordinatorMessageKeyToJson(coordinatorStreamMessage.getType(), coordinatorStreamMessage.getKey());
                if (envelope.getMessage() != null) {
                    // === START MODIFICATION FOR STREAMTEAM PROJECT ===
                    if (envelope.getMessage() instanceof KafkaMessageWithLogAppendTimestamp) {
                        KafkaMessageWithLogAppendTimestamp kafkaMessageWithLogAppendTimestamp = (KafkaMessageWithLogAppendTimestamp) envelope.getMessage();
                        if (kafkaMessageWithLogAppendTimestamp.getMessage() != null) {
                            this.messagesReadFromCoordinatorStream.put(namespacedKey, (byte[]) kafkaMessageWithLogAppendTimestamp.getMessage());
                        } else {
                            this.messagesReadFromCoordinatorStream.remove(namespacedKey);
                        }
                    } else {
                        this.messagesReadFromCoordinatorStream.put(namespacedKey, (byte[]) envelope.getMessage());
                    }
                    // === END MODIFICATION FOR STREAMTEAM PROJECT ===
                } else {
                    this.messagesReadFromCoordinatorStream.remove(namespacedKey);
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            LOG.info("Stopping the coordinator stream system consumer.", this.config);
            this.systemAdmin.stop();
            this.systemProducer.stop();
            this.systemConsumer.stop();
        } catch (Exception e) {
            LOG.error("Exception occurred when closing the metadata store:", e);
        }
    }

    @Override
    public void flush() {
        try {
            this.systemProducer.flush(SOURCE);
        } catch (Exception e) {
            LOG.error("Exception occurred when flushing the metadata store:", e);
            throw new SamzaException("Exception occurred when flushing the metadata store:", e);
        }
    }

    /**
     * <p>
     * Fetches the metadata of the topic partition of coordinator stream. Registers the oldest offset
     * for the topic partition of coordinator stream with the coordinator system consumer.
     * </p>
     */
    private void registerConsumer() {
        LOG.debug("Attempting to register system stream partition: {}", this.coordinatorSystemStreamPartition);
        String streamName = this.coordinatorSystemStreamPartition.getStream();
        Map<String, SystemStreamMetadata> systemStreamMetadataMap = this.systemAdmin.getSystemStreamMetadata(Sets.newHashSet(streamName));

        SystemStreamMetadata systemStreamMetadata = systemStreamMetadataMap.get(streamName);
        Preconditions.checkNotNull(systemStreamMetadata, String.format("System stream metadata does not exist for stream: %s.", streamName));

        SystemStreamPartitionMetadata systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata().get(this.coordinatorSystemStreamPartition.getPartition());
        Preconditions.checkNotNull(systemStreamPartitionMetadata, String.format("System stream partition metadata does not exist for: %s.", this.coordinatorSystemStreamPartition));

        String startingOffset = systemStreamPartitionMetadata.getOldestOffset();
        LOG.info("Registering system stream partition: {} with offset: {}.", this.coordinatorSystemStreamPartition, startingOffset);
        this.systemConsumer.register(this.coordinatorSystemStreamPartition, startingOffset);
    }

    /**
     * Serializes the {@link CoordinatorMessageKey} into a json string.
     *
     * @param type the type of the coordinator message.
     * @param key  the key associated with the type
     * @return the CoordinatorMessageKey serialized to a json string.
     */
    public static String serializeCoordinatorMessageKeyToJson(String type, String key) {
        try {
            CoordinatorMessageKey coordinatorMessageKey = new CoordinatorMessageKey(key, type);
            return OBJECT_MAPPER.writeValueAsString(coordinatorMessageKey);
        } catch (IOException e) {
            throw new SamzaException(String.format("Exception occurred when serializing metadata for type: %s, key: %s", type, key), e);
        }
    }

    /**
     * Deserializes the @param coordinatorMsgKeyAsString in json format to {@link CoordinatorMessageKey}.
     *
     * @param coordinatorMsgKeyAsJson the serialized CoordinatorMessageKey in json format.
     * @return the deserialized CoordinatorMessageKey.
     */
    public static CoordinatorMessageKey deserializeCoordinatorMessageKeyFromJson(String coordinatorMsgKeyAsJson) {
        try {
            return OBJECT_MAPPER.readValue(coordinatorMsgKeyAsJson, CoordinatorMessageKey.class);
        } catch (IOException e) {
            throw new SamzaException(String.format("Exception occurred when deserializing the coordinatorMsgKey: %s", coordinatorMsgKeyAsJson), e);
        }
    }

    /**
     * <p>
     * Represents the key of a message in the coordinator stream.
     * <p>
     * Coordinator message key is composite. It has both the type of the message
     * and the key associated with the type in it.
     * </p>
     */
    public static class CoordinatorMessageKey {

        // Represents the key associated with the type
        private final String key;

        // Represents the type of the message.
        private final String namespace;

        CoordinatorMessageKey(@JsonProperty("key") String key,
                              @JsonProperty("namespace") String namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        public String getKey() {
            return this.key;
        }

        public String getNamespace() {
            return this.namespace;
        }
    }
}