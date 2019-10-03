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

/* Copied from https://github.com/apache/samza/blob/0.13.1/samza-kafka/src/main/scala/org/apache/samza/system/kafka/KafkaSystemConsumer.scala (published under the Apache License)
 * Changes:
 * - Highlighted modifications in addMessage method
 * - Automatic code reformatting by IntelliJ
 */


package org.apache.samza.system.kafka

import java.util.concurrent.ConcurrentHashMap

import ch.unibas.dmi.dbis.streamTeam.samzaExtensions.KafkaMessageWithLogAppendTimestamp
import kafka.api.TopicMetadata
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.message.{Message, MessageAndOffset}
import kafka.serializer.{Decoder, DefaultDecoder}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Utils
import org.apache.samza.Partition
import org.apache.samza.system.{IncomingMessageEnvelope, SystemAdmin, SystemStreamPartition}
import org.apache.samza.util._

import scala.collection.JavaConverters._

object KafkaSystemConsumer {

  // Approximate additional shallow heap overhead per message in addition to the raw bytes
  // received from Kafka  4 + 64 + 4 + 4 + 4 = 80 bytes overhead.
  // As this overhead is a moving target, and not very large
  // compared to the message size its being ignore in the computation for now.
  val MESSAGE_SIZE_OVERHEAD = 4 + 64 + 4 + 4 + 4;

  def toTopicAndPartition(systemStreamPartition: SystemStreamPartition) = {
    val topic = systemStreamPartition.getStream
    val partitionId = systemStreamPartition.getPartition.getPartitionId
    TopicAndPartition(topic, partitionId)
  }
}

/**
 * Maintain a cache of BrokerProxies, returning the appropriate one for the
 * requested topic and partition.
 */
private[kafka] class KafkaSystemConsumer(
                                          systemName: String,
                                          systemAdmin: SystemAdmin,
                                          metrics: KafkaSystemConsumerMetrics,
                                          metadataStore: TopicMetadataStore,
                                          clientId: String,
                                          timeout: Int = ConsumerConfig.ConsumerTimeoutMs,
                                          bufferSize: Int = ConsumerConfig.SocketBufferSize,
                                          fetchSize: StreamFetchSizes = new StreamFetchSizes,
                                          consumerMinSize: Int = ConsumerConfig.MinFetchBytes,
                                          consumerMaxWait: Int = ConsumerConfig.MaxFetchWaitMs,

                                          /**
                                           * Defines a low water mark for how many messages we buffer before we start
                                           * executing fetch requests against brokers to get more messages. This value
                                           * is divided equally among all registered SystemStreamPartitions. For
                                           * example, if fetchThreshold is set to 50000, and there are 50
                                           * SystemStreamPartitions registered, then the per-partition threshold is
                                           * 1000. As soon as a SystemStreamPartition's buffered message count drops
                                           * below 1000, a fetch request will be executed to get more data for it.
                                           *
                                           * Increasing this parameter will decrease the latency between when a queue
                                           * is drained of messages and when new messages are enqueued, but also leads
                                           * to an increase in memory usage since more messages will be held in memory.
                                           */
                                          fetchThreshold: Int = 50000,

                                          /**
                                           * Defines a low water mark for how many bytes we buffer before we start
                                           * executing fetch requests against brokers to get more messages. This
                                           * value is divided by 2 because the messages are buffered twice, once in
                                           * KafkaConsumer and then in SystemConsumers. This value
                                           * is divided equally among all registered SystemStreamPartitions.
                                           * However this is a soft limit per partition, as the
                                           * bytes are cached at the message boundaries, and the actual usage can be
                                           * 1000 bytes + size of max message in the partition for a given stream.
                                           * The bytes if the size of the bytebuffer in Message. Hence, the
                                           * Object overhead is not taken into consideration. In this codebase
                                           * it seems to be quite small. Hence, even for 500000 messages this is around 4MB x 2 = 8MB,
                                           * which is not considerable.
                                           *
                                           * For example,
                                           * if fetchThresholdBytes is set to 100000 bytes, and there are 50
                                           * SystemStreamPartitions registered, then the per-partition threshold is
                                           * (100000 / 2) / 50 = 1000 bytes.
                                           * As this is a soft limit, the actual usage can be 1000 bytes + size of max message.
                                           * As soon as a SystemStreamPartition's buffered messages bytes drops
                                           * below 1000, a fetch request will be executed to get more data for it.
                                           *
                                           * Increasing this parameter will decrease the latency between when a queue
                                           * is drained of messages and when new messages are enqueued, but also leads
                                           * to an increase in memory usage since more messages will be held in memory.
                                           *
                                           * The default value is -1, which means this is not used. When the value
                                           * is > 0, then the fetchThreshold which is count based is ignored.
                                           */
                                          fetchThresholdBytes: Long = -1,

                                          /**
                                           * if(fetchThresholdBytes > 0) true else false
                                           */
                                          fetchLimitByBytesEnabled: Boolean = false,
                                          offsetGetter: GetOffset = new GetOffset("fail"),
                                          deserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
                                          keyDeserializer: Decoder[Object] = new DefaultDecoder().asInstanceOf[Decoder[Object]],
                                          retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                                          clock: () => Long = {
                                            System.currentTimeMillis
                                          }) extends BlockingEnvelopeMap(
  metrics.registry,
  new Clock {
    def currentTimeMillis = clock()
  },
  classOf[KafkaSystemConsumerMetrics].getName) with Toss with Logging {

  type HostPort = (String, Int)
  val brokerProxies = scala.collection.mutable.Map[HostPort, BrokerProxy]()
  val topicPartitionsAndOffsets: scala.collection.concurrent.Map[TopicAndPartition, String] = new ConcurrentHashMap[TopicAndPartition, String]().asScala
  var perPartitionFetchThreshold = fetchThreshold
  var perPartitionFetchThresholdBytes = 0L

  def start() {
    if (topicPartitionsAndOffsets.size > 0) {
      perPartitionFetchThreshold = fetchThreshold / topicPartitionsAndOffsets.size
      // messages get double buffered, hence divide by 2
      if (fetchLimitByBytesEnabled) {
        perPartitionFetchThresholdBytes = (fetchThresholdBytes / 2) / topicPartitionsAndOffsets.size
      }
    }

    refreshBrokers
  }

  override def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    super.register(systemStreamPartition, offset)

    val topicAndPartition = KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition)
    val existingOffset = topicPartitionsAndOffsets.getOrElseUpdate(topicAndPartition, offset)
    // register the older offset in the consumer
    if (systemAdmin.offsetComparator(existingOffset, offset) >= 0) {
      topicPartitionsAndOffsets.replace(topicAndPartition, offset)
    }

    metrics.registerTopicAndPartition(KafkaSystemConsumer.toTopicAndPartition(systemStreamPartition))
  }

  def stop() {
    brokerProxies.values.foreach(_.stop)
  }

  protected def createBrokerProxy(host: String, port: Int): BrokerProxy = {
    new BrokerProxy(host, port, systemName, clientId, metrics, sink, timeout, bufferSize, fetchSize, consumerMinSize, consumerMaxWait, offsetGetter)
  }

  protected def getHostPort(topicMetadata: TopicMetadata, partition: Int): Option[(String, Int)] = {
    // Whatever we do, we can't say Broker, even though we're
    // manipulating it here. Broker is a private type and Scala doesn't seem
    // to care about that as long as you don't explicitly declare its type.
    val brokerOption = topicMetadata
      .partitionsMetadata
      .find(_.partitionId == partition)
      .flatMap(_.leader)

    brokerOption match {
      case Some(broker) => Some(broker.host, broker.port)
      case _ => None
    }
  }

  def refreshBrokers {
    var tpToRefresh = topicPartitionsAndOffsets.keySet.toList
    info("Refreshing brokers for: %s" format topicPartitionsAndOffsets)
    retryBackoff.run(
      loop => {
        val topics = tpToRefresh.map(_.topic).toSet
        val topicMetadata = TopicMetadataCache.getTopicMetadata(topics, systemName, (topics: Set[String]) => metadataStore.getTopicInfo(topics))

        // addTopicPartition one at a time, leaving the to-be-done list intact in case of exceptions.
        // This avoids trying to re-add the same topic partition repeatedly
        def refresh(tp: List[TopicAndPartition]) = {
          val head :: rest = tpToRefresh
          // refreshBrokers can be called from abdicate and refreshDropped,
          // both of which are triggered from BrokerProxy threads. To prevent
          // accidentally creating multiple objects for the same broker, or
          // accidentally not updating the topicPartitionsAndOffsets variable,
          // we need to lock.
          this.synchronized {
            // Check if we still need this TopicAndPartition inside the
            // critical section. If we don't, then notAValidEvent it.
            topicPartitionsAndOffsets.get(head) match {
              case Some(nextOffset) =>
                getHostPort(topicMetadata(head.topic), head.partition) match {
                  case Some((host, port)) =>
                    val brokerProxy = brokerProxies.getOrElseUpdate((host, port), createBrokerProxy(host, port))
                    brokerProxy.addTopicPartition(head, Option(nextOffset))
                    brokerProxy.start
                    debug("Claimed topic-partition (%s) for (%s)".format(head, brokerProxy))
                    topicPartitionsAndOffsets -= head
                  case None => info("No metadata available for: %s. Will try to refresh and add to a consumer thread later." format head)
                }
              case _ => debug("Ignoring refresh for %s because we already added it from another thread." format head)
            }
          }
          rest
        }

        while (!tpToRefresh.isEmpty) {
          tpToRefresh = refresh(tpToRefresh)
        }

        loop.done
      },

      (exception, loop) => {
        warn("While refreshing brokers for %s: %s. Retrying." format(tpToRefresh.head, exception))
        debug("Exception detail:", exception)
      })
  }

  val sink = new MessageSink {
    var lastDroppedRefresh = clock()

    def refreshDropped() {
      if (topicPartitionsAndOffsets.size > 0 && clock() - lastDroppedRefresh > 10000) {
        refreshBrokers
        lastDroppedRefresh = clock()
      }
    }

    def setIsAtHighWatermark(tp: TopicAndPartition, isAtHighWatermark: Boolean) {
      setIsAtHead(toSystemStreamPartition(tp), isAtHighWatermark)
    }

    def needsMoreMessages(tp: TopicAndPartition) = {
      if (fetchLimitByBytesEnabled) {
        getMessagesSizeInQueue(toSystemStreamPartition(tp)) < perPartitionFetchThresholdBytes
      } else {
        getNumMessagesInQueue(toSystemStreamPartition(tp)) < perPartitionFetchThreshold
      }
    }

    def getMessageSize(message: Message): Integer = {
      message.size + KafkaSystemConsumer.MESSAGE_SIZE_OVERHEAD
    }

    def addMessage(tp: TopicAndPartition, msg: MessageAndOffset, highWatermark: Long) = {
      trace("Incoming message %s: %s." format(tp, msg))

      val systemStreamPartition = toSystemStreamPartition(tp)
      val isAtHead = highWatermark == msg.offset
      val offset = msg.offset.toString
      val key = if (msg.message.key != null) {
        keyDeserializer.fromBytes(Utils.readBytes(msg.message.key))
      } else {
        null
      }
      val message = if (!msg.message.isNull) {
        // === START MODIFICATION FOR STREAMTEAM PROJECT ===
        if (msg.message.timestampType == TimestampType.LOG_APPEND_TIME) {
          new KafkaMessageWithLogAppendTimestamp(deserializer.fromBytes(Utils.readBytes(msg.message.payload)), msg.message.timestamp)
        } else {
          new KafkaMessageWithLogAppendTimestamp(deserializer.fromBytes(Utils.readBytes(msg.message.payload)), null)
        }
        // === END MODIFICATION FOR STREAMTEAM PROJECT ===
      } else {
        null
      }

      if (fetchLimitByBytesEnabled) {
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message, getMessageSize(msg.message)))
      } else {
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, offset, key, message))
      }

      setIsAtHead(systemStreamPartition, isAtHead)
    }

    def abdicate(tp: TopicAndPartition, nextOffset: Long) {
      info("Abdicating for %s" format (tp))
      topicPartitionsAndOffsets += tp -> nextOffset.toString
      refreshBrokers
    }

    private def toSystemStreamPartition(tp: TopicAndPartition) = {
      new SystemStreamPartition(systemName, tp.topic, new Partition(tp.partition))
    }
  }
}