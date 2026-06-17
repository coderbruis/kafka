/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.CloseableIterator;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * {@link CompletedFetch} represents a {@link RecordBatch batch} of {@link Record records} that was returned from the
 * broker via a {@link FetchRequest}. It contains logic to maintain state between calls to
 * {@link #fetchRecords(FetchConfig, Deserializers, int)}.
 */
public class CompletedFetch {

    final TopicPartition partition;
    final FetchResponseData.PartitionData partitionData;

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final BufferSupplier decompressionBufferSupplier;
    private final Iterator<? extends RecordBatch> batches;
    private final Set<Long> abortedProducerIds;
    private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
    private final FetchMetricsAggregator metricAggregator;

    private int recordsRead;
    private int bytesRead;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private long nextFetchOffset;
    private Optional<Integer> lastEpoch;
    private volatile boolean isConsumed = false;
    private boolean exhausted = false;
    private boolean initialized = false;

    CompletedFetch(Logger log,
                   SubscriptionState subscriptions,
                   BufferSupplier decompressionBufferSupplier,
                   TopicPartition partition,
                   FetchResponseData.PartitionData partitionData,
                   FetchMetricsAggregator metricAggregator,
                   Long fetchOffset) {
        this.log = log;
        this.subscriptions = subscriptions;
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        this.partition = partition;
        this.partitionData = partitionData;
        this.metricAggregator = metricAggregator;
        this.batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
        this.nextFetchOffset = fetchOffset;
        this.lastEpoch = Optional.empty();
        this.abortedProducerIds = new HashSet<>();
        this.abortedTransactions = abortedTransactions(partitionData);
    }

    long nextFetchOffset() {
        return nextFetchOffset;
    }

    Optional<Integer> lastEpoch() {
        return lastEpoch;
    }

    boolean isInitialized() {
        return initialized;
    }

    void setInitialized() {
        this.initialized = true;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    boolean isExhausted() {
        return exhausted;
    }


    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    void recordAggregatedMetrics(int bytes, int records) {
        metricAggregator.record(partition, bytes, records);
    }

    /**
     * Draining a {@link CompletedFetch} will signal that the data has been consumed and the underlying resources
     * are closed. This is somewhat analogous to {@link Closeable#close() closing}, though no error will result if a
     * caller invokes {@link #fetchRecords(FetchConfig, Deserializers, int)}; an empty {@link List list} will be
     * returned instead.
     */
    void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            this.isConsumed = true;
            recordAggregatedMetrics(bytesRead, recordsRead);

            // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
            // for the same topic can remain together (allowing for more efficient serialization).
            if (bytesRead > 0)
                subscriptions.movePartitionToEnd(partition);
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, RecordBatch batch) {
        if (fetchConfig.checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record batch for partition " + partition + " at offset " +
                        batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, Record record) {
        if (fetchConfig.checkCrcs) {
            try {
                record.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            records.close();
            records = null;
        }
    }

    /**
     * 从 broker 返回的原始 RecordBatch 里，找到下一条可以返回给用户的普通消息 record。
     * 它会跳过：
     * - 已经过期的旧 offset record
     * - control record
     * - read_committed 下已 abort 事务的数据
     * - 空 batch / 已读完的 batch
     * 同时它会：
     * - 校验 batch / record 是否损坏
     * - 更新 nextFetchOffset
     * - 更新当前 batch 的 leader epoch
     * - 标记这批 fetch 是否已经读完
     *
     * nextFetchedRecord() 是 CompletedFetch 内部的底层游标方法。
     * 它负责在原始 RecordBatch 中向前扫描，跳过 Kafka 内部不可见的数据，最终返回下一条用户应该看到的消息 record；如果没有更多消息，就返回 null 并标记 exhausted。
     */
    private Record nextFetchedRecord(FetchConfig fetchConfig) {
        while (true) {
            // 如果当前 batch 没有正在使用的 record iterator，或者当前 batch 已经读完，就准备切换到下一个 batch。
            if (records == null || !records.hasNext()) {
                // 关闭当前 batch 的 record iterator，释放解压相关资源。
                maybeCloseRecordStream();
                // 如果没有更多 batch 了，说明这个 CompletedFetch 快读完了。
                if (!batches.hasNext()) {
                    // Message format v2 preserves the last offset in a batch even if the last record is removed
                    // through compaction. By using the next offset computed from the last offset in the batch,
                    // we ensure that the offset of the next fetch will point to the next batch, which avoids
                    // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    // fetching the same batch repeatedly).

                    // 如果之前处理过 batch，把 nextFetchOffset 推到当前 batch 的下一个 offset。
                    // 这对压缩/compact 场景很重要：即使 batch 中最后一条实际 record 被删除，也要用 batch 级别的 nextOffset() 前进，避免反复 fetch 同一个 batch。
                    if (currentBatch != null)
                        nextFetchOffset = currentBatch.nextOffset();
                    // 标记这批 fetch 数据已经读尽。
                    exhausted = true;
                    return null;
                }

                // 切换到下一个 RecordBatch。
                currentBatch = batches.next();
                // 记录当前 batch 的 leader epoch。后续更新消费 position 时会用到。
                lastEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                // 如果开启 CRC 校验，就校验整个 batch 是否有效。
                maybeEnsureValid(fetchConfig, currentBatch);

                // 如果消费者使用 read_committed，并且当前 batch 属于某个 producer，就需要处理事务可见性。
                if (fetchConfig.isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // remove from the aborted transaction queue all aborted transactions which have begun
                    // before the current batch's last offset and add the associated producerIds to the
                    // aborted producer set
                    // 把 abort transaction 列表中，起始 offset 不超过当前 batch last offset 的事务消费出来，并记录对应 producer id。
                    // 这样后面可以判断当前 batch 是否属于已 abort 事务。
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                    long producerId = currentBatch.producerId();
                    // 如果当前 batch 是 abort marker，说明这个 producer 的 abort 事务范围结束了，从 aborted producer 集合里移除。
                    if (containsAbortMarker(currentBatch)) {
                        abortedProducerIds.remove(producerId);
                    } else if (isBatchAborted(currentBatch)) {
                        log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                        "offsets {} to {}",
                                partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                        // 把 offset 推进到这个 aborted batch 后面。
                        nextFetchOffset = currentBatch.nextOffset();
                        continue;
                    }
                }
                // 为当前 batch 创建 record iterator。如果 batch 是压缩的，这里会用 decompressionBufferSupplier 进行流式解压。
                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {        // 进入这里表示当前 batch 的 record iterator 还有数据。
                Record record = records.next();
                // skip any records out of range
                // 只处理 offset 没有过期的 record。如果 record offset 小于 nextFetchOffset，说明它已经被消费过或应该跳过。
                if (record.offset() >= nextFetchOffset) {
                    // we only do validation when the message should not be skipped.
                    maybeEnsureValid(fetchConfig, record);

                    // control records are not returned to the user
                    // 如果不是 control batch，就返回这条普通消息 record。
                    // 这是用户最终能看到的消息。
                    if (!currentBatch.isControlBatch()) {
                        return record;
                    } else {
                        // Increment the next fetch offset when we skip a control batch.
                        // 如果是 control record，不返回给用户，但要推进 offset。
                        // control record 是 Kafka 内部事务控制消息，例如 commit/abort marker。
                        nextFetchOffset = record.offset() + 1;
                    }
                }
            }
        }
    }

    /**
     * 从一个已经 fetch 回来的分区数据里，最多取出 maxRecords 条消息，校验记录、跳过不该返回的记录、反序列化 key/value，最后生成 ConsumerRecord 列表。
     *
     * The {@link RecordBatch batch} of {@link Record records} is converted to a {@link List list} of
     * {@link ConsumerRecord consumer records} and returned. {@link BufferSupplier Decompression} and
     * {@link Deserializer deserialization} of the {@link Record record's} key and value are performed in
     * this step.
     *
     * @param fetchConfig {@link FetchConfig Configuration} to use
     * @param deserializers {@link Deserializer}s to use to convert the raw bytes to the expected key and value types
     * @param maxRecords The number of records to return; the number returned may be {@code 0 <= maxRecords}
     * @return {@link ConsumerRecord Consumer records}
     */
    <K, V> List<ConsumerRecord<K, V>> fetchRecords(FetchConfig fetchConfig,
                                                   Deserializers<K, V> deserializers,
                                                   int maxRecords) {
        // Error when fetching the next record before deserialization.
        // 如果上一次是在“读取下一条原始 record”阶段失败，比如 batch 损坏，就直接抛异常。
        // 这种情况下没法靠重新反序列化恢复，只能提示用户必要时 seek 跳过坏 record。
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);
        // 如果这个 CompletedFetch 已经被 drain/消费完了，就直接返回空列表。
        if (isConsumed)
            return Collections.emptyList();

        List<ConsumerRecord<K, V>> records = new ArrayList<>();

        try {
            for (int i = 0; i < maxRecords; i++) {
                // Only move to next record if there was no exception in the last fetch. Otherwise, we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    // 读取下一条可返回的原始 record。
                    lastRecord = nextFetchedRecord(fetchConfig);
                    corruptLastRecord = false;
                }

                if (lastRecord == null)
                    break;
                // 取当前 batch 的 leader epoch，后面放进 ConsumerRecord。
                Optional<Integer> leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                TimestampType timestampType = currentBatch.timestampType();
                // 反序列化 key 和 value，把底层原始 Record 转成用户看到的 ConsumerRecord。
                ConsumerRecord<K, V> record = parseRecord(deserializers, partition, leaderEpoch, timestampType, lastRecord);
                records.add(record);
                // 统计record数
                recordsRead++;
                // 统计已读字节数
                bytesRead += lastRecord.sizeInBytes();
                // 推进下次fetch的offset
                nextFetchOffset = lastRecord.offset() + 1;
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (records.isEmpty())
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (records.isEmpty())
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        }
        return records;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    <K, V> ConsumerRecord<K, V> parseRecord(Deserializers<K, V> deserializers,
                                            TopicPartition partition,
                                            Optional<Integer> leaderEpoch,
                                            TimestampType timestampType,
                                            Record record) {
        ByteBuffer keyBytes = record.key();
        ByteBuffer valueBytes = record.value();
        Headers headers = new RecordHeaders(record.headers());
        K key;
        V value;
        try {
            key = keyBytes == null ? null : deserializers.keyDeserializer().deserialize(partition.topic(), headers, keyBytes);
        } catch (RuntimeException e) {
            log.error("Key Deserializers with error: {}", deserializers);
            throw newRecordDeserializationException(DeserializationExceptionOrigin.KEY, partition, timestampType, record, e, headers);
        }
        try {
            value = valueBytes == null ? null : deserializers.valueDeserializer().deserialize(partition.topic(), headers, valueBytes);
        } catch (RuntimeException e) {
            log.error("Value Deserializers with error: {}", deserializers);
            throw newRecordDeserializationException(DeserializationExceptionOrigin.VALUE, partition, timestampType, record, e, headers);
        }
        return new ConsumerRecord<>(partition.topic(), partition.partition(), record.offset(),
                record.timestamp(), timestampType,
                keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(),
                valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(),
                key, value, headers, leaderEpoch);
    }

    private static RecordDeserializationException newRecordDeserializationException(DeserializationExceptionOrigin origin,
                                                                                    TopicPartition partition,
                                                                                    TimestampType timestampType,
                                                                                    Record record,
                                                                                    RuntimeException e,
                                                                                    Headers headers) {
        return new RecordDeserializationException(origin, partition, record.offset(), record.timestamp(), timestampType, record.key(), record.value(), headers,
                "Error deserializing " + origin.name() + " for partition " + partition + " at offset " + record.offset()
                        + ". If needed, please seek past the record to continue consumption.", e);
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    private void consumeAbortedTransactionsUpTo(long offset) {
        if (abortedTransactions == null)
            return;

        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
            FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            abortedProducerIds.add(abortedTransaction.producerId());
        }
    }

    private boolean isBatchAborted(RecordBatch batch) {
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }

    private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(FetchResponseData.PartitionData partition) {
        if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
            return null;

        PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                partition.abortedTransactions().size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
        );
        abortedTransactions.addAll(partition.abortedTransactions());
        return abortedTransactions;
    }

    private boolean containsAbortMarker(RecordBatch batch) {
        if (!batch.isControlBatch())
            return false;

        Iterator<Record> batchIterator = batch.iterator();
        if (!batchIterator.hasNext())
            return false;

        Record firstRecord = batchIterator.next();
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    }
}
