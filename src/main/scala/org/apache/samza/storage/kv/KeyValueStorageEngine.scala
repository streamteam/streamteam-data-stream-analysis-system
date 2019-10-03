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

/* Copied from https://github.com/apache/samza/blob/0.13.1/samza-kv/src/main/scala/org/apache/samza/storage/kv/KeyValueStorageEngine.scala (published under the Apache License)
 * Changes:
 * - Highlighted modifications in restore method
 * - Automatic code reformatting by IntelliJ
 */

package org.apache.samza.storage.kv

import ch.unibas.dmi.dbis.streamTeam.samzaExtensions.KafkaMessageWithLogAppendTimestamp
import org.apache.samza.storage.{StorageEngine, StoreProperties}
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.util.{Logging, TimerUtils}

import scala.collection.JavaConverters._

/**
 * A key value store.
 *
 * This implements both the key/value interface and the storage engine interface.
 */
class KeyValueStorageEngine[K, V](
                                   storeProperties: StoreProperties,
                                   wrapperStore: KeyValueStore[K, V],
                                   rawStore: KeyValueStore[Array[Byte], Array[Byte]],
                                   metrics: KeyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics,
                                   batchSize: Int = 500,
                                   val clock: () => Long = {
                                     System.nanoTime
                                   }) extends StorageEngine with KeyValueStore[K, V] with TimerUtils with Logging {

  var count = 0

  /* delegate to underlying store */
  def get(key: K): V = {
    updateTimer(metrics.getNs) {
      metrics.gets.inc

      //update the duration and return the fetched value
      wrapperStore.get(key)
    }
  }

  def getAll(keys: java.util.List[K]): java.util.Map[K, V] = {
    metrics.gets.inc(keys.size)
    wrapperStore.getAll(keys)
  }

  def put(key: K, value: V) = {
    updateTimer(metrics.putNs) {
      metrics.puts.inc
      wrapperStore.put(key, value)
    }
  }

  def putAll(entries: java.util.List[Entry[K, V]]) = {
    metrics.puts.inc(entries.size)
    wrapperStore.putAll(entries)
  }

  def delete(key: K) = {
    updateTimer(metrics.deleteNs) {
      metrics.deletes.inc
      wrapperStore.delete(key)
    }
  }

  def deleteAll(keys: java.util.List[K]) = {
    metrics.deletes.inc(keys.size)
    wrapperStore.deleteAll(keys)
  }

  def range(from: K, to: K) = {
    updateTimer(metrics.rangeNs) {
      metrics.ranges.inc
      wrapperStore.range(from, to)
    }
  }

  def all() = {
    updateTimer(metrics.allNs) {
      metrics.alls.inc
      wrapperStore.all()
    }
  }

  /**
   * Restore the contents of this key/value store from the change log,
   * batching updates to underlying raw store to notAValidEvent wrapping functions for efficiency.
   */
  def restore(envelopes: java.util.Iterator[IncomingMessageEnvelope]) {
    val batch = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](batchSize)

    for (envelope <- envelopes.asScala) {
      val keyBytes = envelope.getKey.asInstanceOf[Array[Byte]]

      // === START MODIFICATION FOR STREAMTEAM PROJECT ===
      val bareMessage = if (envelope.getMessage.isInstanceOf[KafkaMessageWithLogAppendTimestamp]) {
        KafkaMessageWithLogAppendTimestamp.getMessageFromIncomingMessageEnvelope(envelope)
      } else {
        envelope.getMessage
      }

      val valBytes = bareMessage.asInstanceOf[Array[Byte]]
      // === END MODIFICATION FOR STREAMTEAM PROJECT ===

      batch.add(new Entry(keyBytes, valBytes))

      if (batch.size >= batchSize) {
        rawStore.putAll(batch)
        batch.clear()
      }

      if (valBytes != null) {
        metrics.restoredBytes.inc(valBytes.size)
      }

      metrics.restoredBytes.inc(keyBytes.size)
      metrics.restoredMessages.inc
      count += 1

      if (count % 1000000 == 0) {
        info(count + " entries restored...")
      }
    }

    if (batch.size > 0) {
      rawStore.putAll(batch)
    }
  }

  def flush() = {
    updateTimer(metrics.flushNs) {
      trace("Flushing.")
      metrics.flushes.inc
      wrapperStore.flush()
    }
  }

  def stop() = {
    trace("Stopping.")

    close()
  }

  def close() = {
    trace("Closing.")

    flush()
    wrapperStore.close()
  }

  override def getStoreProperties: StoreProperties = storeProperties
}