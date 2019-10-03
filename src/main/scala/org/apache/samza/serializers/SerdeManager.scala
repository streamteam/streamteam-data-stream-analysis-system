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

/* Copied from https://github.com/apache/samza/blob/0.13.1/samza-core/src/main/scala/org/apache/samza/serializers/SerdeManager.scala (published under the Apache License)
 * Changes:
 * - Highlighted modifications in fromBytes method
 * - Automatic code reformatting by IntelliJ
 */

package org.apache.samza.serializers

import ch.unibas.dmi.dbis.streamTeam.samzaExtensions.KafkaMessageWithLogAppendTimestamp
import org.apache.samza.SamzaException
import org.apache.samza.config.StorageConfig
import org.apache.samza.message.ControlMessage
import org.apache.samza.system.{IncomingMessageEnvelope, OutgoingMessageEnvelope, SystemStream}

class SerdeManager(
                    serdes: Map[String, Serde[Object]] = Map(),
                    systemKeySerdes: Map[String, Serde[Object]] = Map(),
                    systemMessageSerdes: Map[String, Serde[Object]] = Map(),
                    systemStreamKeySerdes: Map[SystemStream, Serde[Object]] = Map(),
                    systemStreamMessageSerdes: Map[SystemStream, Serde[Object]] = Map(),
                    changeLogSystemStreams: Set[SystemStream] = Set(),
                    controlMessageKeySerdes: Map[SystemStream, Serde[String]] = Map(),
                    intermediateMessageSerdes: Map[SystemStream, Serde[Object]] = Map()) {

  def toBytes(obj: Object, serializerName: String) = serdes
    .getOrElse(serializerName, throw new SamzaException("No serde defined for %s" format serializerName))
    .toBytes(obj)

  def toBytes(envelope: OutgoingMessageEnvelope): OutgoingMessageEnvelope = {
    val key = if (changeLogSystemStreams.contains(envelope.getSystemStream)
      || envelope.getSystemStream.getStream.endsWith(StorageConfig.ACCESSLOG_STREAM_SUFFIX)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getKey
    } else if (envelope.getMessage.isInstanceOf[ControlMessage]
      && controlMessageKeySerdes.contains(envelope.getSystemStream)) {
      // If the message is a control message and the key needs to serialize
      controlMessageKeySerdes(envelope.getSystemStream).toBytes(envelope.getKey.asInstanceOf[String])
    } else if (envelope.getKeySerializerName != null) {
      // If a serde is defined, use it.
      toBytes(envelope.getKey, envelope.getKeySerializerName)
    } else if (systemStreamKeySerdes.contains(envelope.getSystemStream)) {
      // If the stream has a serde defined, use it.
      systemStreamKeySerdes(envelope.getSystemStream).toBytes(envelope.getKey)
    } else if (systemKeySerdes.contains(envelope.getSystemStream.getSystem)) {
      // If the system has a serde defined, use it.
      systemKeySerdes(envelope.getSystemStream.getSystem).toBytes(envelope.getKey)
    } else {
      // Just use the object.
      envelope.getKey
    }

    val message = if (changeLogSystemStreams.contains(envelope.getSystemStream)
      || envelope.getSystemStream.getStream.endsWith(StorageConfig.ACCESSLOG_STREAM_SUFFIX)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getMessage
    } else if (intermediateMessageSerdes.contains(envelope.getSystemStream)) {
      // If the stream is an intermediate stream, use the intermediate message serde
      intermediateMessageSerdes(envelope.getSystemStream).toBytes(envelope.getMessage)
    } else if (envelope.getMessageSerializerName != null) {
      // If a serde is defined, use it.
      toBytes(envelope.getMessage, envelope.getMessageSerializerName)
    } else if (systemStreamMessageSerdes.contains(envelope.getSystemStream)) {
      // If the stream has a serde defined, use it.
      systemStreamMessageSerdes(envelope.getSystemStream).toBytes(envelope.getMessage)
    } else if (systemMessageSerdes.contains(envelope.getSystemStream.getSystem)) {
      // If the system has a serde defined, use it.
      systemMessageSerdes(envelope.getSystemStream.getSystem).toBytes(envelope.getMessage)
    } else {
      // Just use the object.
      envelope.getMessage
    }

    if ((key eq envelope.getKey) && (message eq envelope.getMessage)) {
      envelope
    } else {
      new OutgoingMessageEnvelope(
        envelope.getSystemStream,
        null,
        null,
        envelope.getPartitionKey,
        key,
        message)
    }
  }

  def fromBytes(bytes: Array[Byte], deserializerName: String) = serdes
    .getOrElse(deserializerName, throw new SamzaException("No serde defined for %s" format deserializerName))
    .fromBytes(bytes)

  def fromBytes(envelope: IncomingMessageEnvelope) = {
    val systemStream = envelope.getSystemStreamPartition.getSystemStream

    // === START MODIFICATION FOR STREAMTEAM PROJECT ===
    val bareMessage = if (envelope.getMessage.isInstanceOf[KafkaMessageWithLogAppendTimestamp]) {
      KafkaMessageWithLogAppendTimestamp.getMessageFromIncomingMessageEnvelope(envelope)
    } else {
      envelope.getMessage
    }

    val deserializedMessage = if (changeLogSystemStreams.contains(systemStream)
      || systemStream.getStream.endsWith(StorageConfig.ACCESSLOG_STREAM_SUFFIX)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      bareMessage
    } else if (intermediateMessageSerdes.contains(systemStream)) {
      // If the stream is an intermediate stream, use the intermediate message serde
      intermediateMessageSerdes(systemStream).fromBytes(bareMessage.asInstanceOf[Array[Byte]])
    } else if (systemStreamMessageSerdes.contains(systemStream)) {
      // If the stream has a serde defined, use it.
      systemStreamMessageSerdes(systemStream).fromBytes(bareMessage.asInstanceOf[Array[Byte]])
    } else if (systemMessageSerdes.contains(systemStream.getSystem)) {
      // If the system has a serde defined, use it.
      systemMessageSerdes(systemStream.getSystem).fromBytes(bareMessage.asInstanceOf[Array[Byte]])
    } else {
      // Just use the object.
      bareMessage
    }

    val message = if (envelope.getMessage.isInstanceOf[KafkaMessageWithLogAppendTimestamp]) {
      new KafkaMessageWithLogAppendTimestamp(deserializedMessage, KafkaMessageWithLogAppendTimestamp.getLogAppendTimestampFromIncomingMessageEnvelope(envelope))
    } else {
      deserializedMessage
    }
    // === END MODIFICATION FOR STREAMTEAM PROJECT ===

    val key = if (changeLogSystemStreams.contains(systemStream)
      || systemStream.getStream.endsWith(StorageConfig.ACCESSLOG_STREAM_SUFFIX)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getKey
    } else if (message.isInstanceOf[ControlMessage]
      && controlMessageKeySerdes.contains(systemStream)) {
      // If the message is a control message and the key needs to deserialize
      controlMessageKeySerdes(systemStream).fromBytes(envelope.getKey.asInstanceOf[Array[Byte]])
    } else if (systemStreamKeySerdes.contains(systemStream)) {
      // If the stream has a serde defined, use it.
      systemStreamKeySerdes(systemStream).fromBytes(envelope.getKey.asInstanceOf[Array[Byte]])
    } else if (systemKeySerdes.contains(systemStream.getSystem)) {
      // If the system has a serde defined, use it.
      systemKeySerdes(systemStream.getSystem).fromBytes(envelope.getKey.asInstanceOf[Array[Byte]])
    } else {
      // Just use the object.
      envelope.getKey
    }

    if ((key eq envelope.getKey) && (message eq envelope.getMessage)) {
      envelope
    } else {
      new IncomingMessageEnvelope(
        envelope.getSystemStreamPartition,
        envelope.getOffset,
        key,
        message)
    }
  }
}