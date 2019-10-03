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

package ch.unibas.dmi.dbis.streamTeam.samzaExtensions;

import org.apache.samza.system.IncomingMessageEnvelope;

/**
 * Wrapper which contains the actual bare message of a received incoming message envelope and the log append time of the corresponding Kafka log append timestamp.
 * Required to access Kafka's log append timestamp in the process() method of AbstractTask and thus to assign ingestion timestamps to raw input stream elements.
 * Samza's KafkaSystemConsumer, CoordinatorStreamSystemConsumer, SerdeManager, and KeyValueStorageEngine are adapted to handle this wrapper instead of the bare message.
 */
public class KafkaMessageWithLogAppendTimestamp {

    /**
     * Bare message
     */
    private final Object message;

    /**
     * Log append timestamp (log.message.timestamp.type has to be set to LogAppendTime in the Kafka config, otherwise always null)
     */
    private final Long logAppendTimestamp;

    /**
     * KafkaMessageWithLogAppendTimestamp constructor.
     *
     * @param message            Bare message
     * @param logAppendTimestamp Log append timestamp (log.message.timestamp.type has to be set to LogAppendTime in the Kafka config, otherwise always null)
     */
    public KafkaMessageWithLogAppendTimestamp(Object message, Long logAppendTimestamp) {
        this.message = message;
        this.logAppendTimestamp = logAppendTimestamp;
    }

    /**
     * Gets the bare message.
     *
     * @return Bare message
     */
    public Object getMessage() {
        return this.message;
    }

    /**
     * Gets the log append timestamp.
     *
     * @return Log append timestamp
     */
    public Long getLogAppendTimestamp() {
        return this.logAppendTimestamp;
    }

    /**
     * Gets the bare message of an incoming message envelope.
     *
     * @param incomingMessageEnvelope Incoming message envelope
     * @return Bare message of an incoming message envelope
     */
    public static Object getMessageFromIncomingMessageEnvelope(IncomingMessageEnvelope incomingMessageEnvelope) {
        KafkaMessageWithLogAppendTimestamp kafkaMessageWithLogAppendTimestamp = (KafkaMessageWithLogAppendTimestamp) incomingMessageEnvelope.getMessage();
        return kafkaMessageWithLogAppendTimestamp.getMessage();
    }

    /**
     * Gets the log append timestamp of an incoming message envelope.
     *
     * @param incomingMessageEnvelope Incoming message envelope
     * @return Log append timestamp of an incoming message envelope
     */
    public static Long getLogAppendTimestampFromIncomingMessageEnvelope(IncomingMessageEnvelope incomingMessageEnvelope) {
        KafkaMessageWithLogAppendTimestamp kafkaMessageWithLogAppendTimestamp = (KafkaMessageWithLogAppendTimestamp) incomingMessageEnvelope.getMessage();
        return kafkaMessageWithLogAppendTimestamp.getLogAppendTimestamp();
    }
}
