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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code FetchCollector} operates at the {@link RecordBatch} level, as that is what is stored in the
 * {@link FetchBuffer}. Each {@link org.apache.kafka.common.record.internal.Record} in the {@link RecordBatch} is converted
 * to a {@link ConsumerRecord} and added to the returned {@link Fetch}.
 *
 * @param <K> Record key type
 * @param <V> Record value type
 */
public class FetchCollector<K, V> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;
    private final Deserializers<K, V> deserializers;
    private final FetchMetricsManager metricsManager;
    private final Time time;

    public FetchCollector(final LogContext logContext,
                          final ConsumerMetadata metadata,
                          final SubscriptionState subscriptions,
                          final FetchConfig fetchConfig,
                          final Deserializers<K, V> deserializers,
                          final FetchMetricsManager metricsManager,
                          final Time time) {
        this.log = logContext.logger(FetchCollector.class);
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.deserializers = deserializers;
        this.metricsManager = metricsManager;
        this.time = time;
    }

    /**
     * 从 FetchBuffer 里取出已经 fetch 回来的数据，把 CompletedFetch 解析成用户可见的 ConsumerRecord，组装成 Fetch<K, V> 返回，同时推进消费位置。
     *
     * Return the fetched {@link ConsumerRecord records}, empty the {@link FetchBuffer record buffer}, and
     * update the consumed position.
     *
     * <p>
     * NOTE: returning an {@link Fetch#empty() empty} fetch guarantees the consumed position is not updated.
     *
     * @param fetchBuffer {@link FetchBuffer} from which to retrieve the {@link ConsumerRecord records}
     *
     * @return A {@link Fetch} for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public Fetch<K, V> collectFetch(final FetchBuffer fetchBuffer) {
        final Fetch<K, V> fetch = Fetch.empty();
        // 临时保存“分区已经暂停”的 fetch 数据。
        // 暂停的分区不能返回 records，但数据不能丢，所以稍后放回 FetchBuffer。
        final Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
        // 本次最多还能返回多少条记录，对应配置max.poll.records。
        int recordsRemaining = fetchConfig.maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                // 拿当前正在处理的 CompletedFetch。
                // 一个 CompletedFetch 可能包含很多 records，本次 poll 不一定一次取完，所以需要 nextInLineFetch 保存“处理到一半”的 fetch。
                final CompletedFetch nextInLineFetch = fetchBuffer.nextInLineFetch();
                // 如果当前没有正在处理的 fetch，或者当前 fetch 已经消费完了，就准备从 buffer 队列里拿新的。
                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    final CompletedFetch completedFetch = fetchBuffer.peek();

                    if (completedFetch == null)
                        break;
                    // 如果这个 CompletedFetch 还没初始化，就先初始化。
                    if (!completedFetch.isInitialized()) {
                        try {
                            fetchBuffer.setNextInLineFetch(initialize(completedFetch));
                        } catch (Exception e) {
                            // Remove a completedFetch upon a parse with exception if (1) it contains no completedFetch, and
                            // (2) there are no fetched completedFetch with actual content preceding this exception.
                            // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                            // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                            // potential data loss due to an exception in a following record.

                            // 如果当前还没有收集到任何 records，并且这个异常 fetch 本身没有实际消息内容，就把它从 buffer 移除。
                            // 目的：避免下次 poll 又卡在同一个异常 fetch 上。
                            if (fetch.isEmpty() && FetchResponse.recordsOrFail(completedFetch.partitionData).sizeInBytes() == 0)
                                fetchBuffer.poll();

                            throw e;
                        }
                    } else {
                        fetchBuffer.setNextInLineFetch(completedFetch);
                    }

                    fetchBuffer.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                    // 常见场景：
                    // 1)业务处理不过来
                    //  某个分区消息很多，应用想先暂停它，等处理完积压后再 resume(...)。
                    // 2)按分区做限流
                    //  比如某个分区对应的下游服务慢了，只暂停这个分区，不影响其他分区继续消费。
                    // 3)异步处理时防止拉太多
                    //  应用 poll 到消息后异步处理，为了避免同一分区继续返回更多消息，可以先 pause，处理完成后再 resume。
                    // 4)临时跳过某些分区
                    //  比如某个分区的数据暂时有问题，应用想先消费其他分区
                    // when the partition is paused we add the records back to the completedFetches queue instead of draining
                    // them so that they can be returned on a subsequent poll if the partition is resumed at that time
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition);
                    pausedCompletedFetches.add(nextInLineFetch);
                    fetchBuffer.setNextInLineFetch(null);
                } else {
                    final Fetch<K, V> nextFetch = fetchRecords(nextInLineFetch, recordsRemaining);
                    recordsRemaining -= nextFetch.numRecords();
                    fetch.add(nextFetch);
                }
            }
        } catch (KafkaException e) {
            if (fetch.isEmpty())
                throw e;
        } finally {
            // add any polled completed fetches for paused partitions back to the completed fetches queue to be
            // re-evaluated in the next poll
            fetchBuffer.addAll(pausedCompletedFetches);
        }

        return fetch;
    }

    /**
     * 从一个已经初始化好的 CompletedFetch 里读取 records，确认这些 records 还能返回给当前 consumer，
     * 然后更新分区消费位置和指标，最后包装成 Fetch 返回。
     */
    private Fetch<K, V> fetchRecords(final CompletedFetch nextInLineFetch, int maxRecords) {
        final TopicPartition tp = nextInLineFetch.partition;
        // 检查这个分区是否还分配给当前 consumer。
        // 如果 rebalance 已经发生，这批数据可能是旧 assignment 时 fetch 回来的。
        if (!subscriptions.isAssigned(tp)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", tp);
        } else if (!subscriptions.isFetchable(tp)) {
            // this can happen when a partition is paused before fetched records are returned to the consumer's
            // poll call or if the offset is being reset.
            // It can also happen under the Consumer rebalance protocol, when the consumer changes its subscription.
            // Until the consumer receives an updated assignment from the coordinator, it can hold assigned partitions
            // that are not in the subscription anymore, so we make them not fetchable.
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", tp);
        } else {
            SubscriptionState.FetchPosition position = subscriptions.position(tp);

            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + tp);
            // 防止过期fetch请求的数据
            if (nextInLineFetch.nextFetchOffset() == position.offset) {
                // 从nextInLineFetch中读取数据并反序列化
                List<ConsumerRecord<K, V>> partRecords = nextInLineFetch.fetchRecords(fetchConfig,
                        deserializers,
                        maxRecords);

                log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                        partRecords.size(), position, tp);
                // 记录本次是否推进了消费位置。
                boolean positionAdvanced = false;
                // 如果读取 records 后(nextFetchOffset会加1)，CompletedFetch 的 next offset 比原 position 大，说明确实消费到了 records。
                if (nextInLineFetch.nextFetchOffset() > position.offset) {
                    // 构造新的消费位置
                    SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                            nextInLineFetch.nextFetchOffset(),
                            nextInLineFetch.lastEpoch(),
                            position.currentLeader);
                    log.trace("Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                            position, nextPosition, tp, partRecords.size());
                    // 更新消费位置
                    subscriptions.position(tp, nextPosition);
                    // 标记position已经前进。
                    positionAdvanced = true;
                }

                // Drain after position update to ensure the background thread sees the updated position
                // before it sees isConsumed=true. This prevents duplicate fetch requests for the old offset.
                // 注释里强调先更新 position，再 drain，是为了让后台线程先看到新 position，避免重复按旧 offset 发 fetch。
                // 这批CompleteFetch读完，就标记为consumed
                if (nextInLineFetch.isExhausted()) {
                    // 标记为consumed
                    nextInLineFetch.drain();
                }

                // 计算当前分区 lag。lag表示：当前消费位置距离 high watermark / last stable offset 还有多远。
                Long partitionLag = subscriptions.partitionLag(tp, fetchConfig.isolationLevel);
                if (partitionLag != null)
                    metricsManager.recordPartitionLag(tp, partitionLag);

                // 计算 partition lead。lead表示当前消费位置距离 log start offset 的距离。
                Long lead = subscriptions.partitionLead(tp);
                if (lead != null) {
                    metricsManager.recordPartitionLead(tp, lead);
                }
                // 把这个分区读到的 records 包装成 Fetch 返回。
                return Fetch.forPartition(tp, partRecords, positionAdvanced, new OffsetAndMetadata(nextInLineFetch.nextFetchOffset(), nextInLineFetch.lastEpoch(), ""));
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                // 这批数据是过期 fetch 请求的结果，不能返回。
                // 例如：
                // 1)用户 seek 过。
                // 2)offset reset 过。
                // 3)rebalance 后 position 变了。
                // 4)旧请求响应回来得太晚。
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        tp, nextInLineFetch.nextFetchOffset(), position);
            }
        }

        log.trace("Draining fetched records for partition {}", tp);
        // 丢弃这批不能返回的数据，标记为 consumed。
        nextInLineFetch.drain();

        return Fetch.empty();
    }

    /**
     * Initialize a CompletedFetch object.
     */
    protected CompletedFetch initialize(final CompletedFetch completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final Errors error = Errors.forCode(completedFetch.partitionData.errorCode());
        boolean recordMetrics = true;

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // this can happen when a rebalance happened while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
                return null;
            } else if (error == Errors.NONE) {
                final CompletedFetch ret = handleInitializeSuccess(completedFetch);
                recordMetrics = ret == null;
                return ret;
            } else {
                handleInitializeErrors(completedFetch, error);
                return null;
            }
        } finally {
            if (recordMetrics) {
                completedFetch.recordAggregatedMetrics(0, 0);
            }

            if (error != Errors.NONE)
                // we move the partition to the end if there was an error. This way, it's more likely that partitions for
                // the same topic can remain together (allowing for more efficient serialization).
                subscriptions.movePartitionToEnd(tp);
        }
    }

    private CompletedFetch handleInitializeSuccess(final CompletedFetch completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset();

        // we are interested in this fetch only if the beginning offset matches the
        // current consumed position
        SubscriptionState.FetchPosition position = subscriptions.positionOrNull(tp);
        if (position == null || position.offset != fetchOffset) {
            log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                "the expected offset {} or the partition has been unassigned", tp, fetchOffset, position);
            return null;
        }

        final FetchResponseData.PartitionData partition = completedFetch.partitionData;
        log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
                FetchResponse.recordsSize(partition), tp, position);
        Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();

        if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
            // This should not happen with brokers that support FetchRequest/Response V4 or higher (i.e. KIP-74)
            throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
                    fetchOffset + ". Received a non-empty fetch response from the server, but no " +
                    "complete records were found.");
        }

        if (!updatePartitionState(partition, tp)) {
            return null;
        }

        completedFetch.setInitialized();
        return completedFetch;
    }

    private boolean updatePartitionState(final FetchResponseData.PartitionData partitionData,
                                         final TopicPartition tp) {
        if (partitionData.highWatermark() >= 0) {
            log.trace("Updating high watermark for partition {} to {}", tp, partitionData.highWatermark());
            if (!subscriptions.tryUpdatingHighWatermark(tp, partitionData.highWatermark())) {
                return false;
            }
        }

        if (partitionData.logStartOffset() >= 0) {
            log.trace("Updating log start offset for partition {} to {}", tp, partitionData.logStartOffset());
            if (!subscriptions.tryUpdatingLogStartOffset(tp, partitionData.logStartOffset())) {
                return false;
            }
        }

        if (partitionData.lastStableOffset() >= 0) {
            log.trace("Updating last stable offset for partition {} to {}", tp, partitionData.lastStableOffset());
            if (!subscriptions.tryUpdatingLastStableOffset(tp, partitionData.lastStableOffset())) {
                return false;
            }
        }

        if (FetchResponse.isPreferredReplica(partitionData)) {
            return subscriptions.tryUpdatingPreferredReadReplica(
                tp, partitionData.preferredReadReplica(), () -> {
                    long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                    log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
                        tp, partitionData.preferredReadReplica(), expireTimeMs);
                    return expireTimeMs;
                });
        }

        return true;
    }

    private void handleInitializeErrors(final CompletedFetch completedFetch, final Errors error) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset();

        if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                error == Errors.REPLICA_NOT_AVAILABLE ||
                error == Errors.KAFKA_STORAGE_ERROR ||
                error == Errors.FENCED_LEADER_EPOCH ||
                error == Errors.OFFSET_NOT_AVAILABLE) {
            log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.UNKNOWN_TOPIC_ID) {
            log.warn("Received unknown topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
            log.warn("Received inconsistent topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
            Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);

            if (clearedReplicaId.isEmpty()) {
                // If there's no preferred replica to clear, we're fetching from the leader so handle this error normally
                SubscriptionState.FetchPosition position = subscriptions.positionOrNull(tp);

                if (position == null || fetchOffset != position.offset) {
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                            "does not match the current offset {} or the partition has been unassigned", tp, fetchOffset, position);
                } else {
                    String errorMessage = "Fetch position " + position + " is out of range for partition " + tp;

                    if (subscriptions.hasDefaultOffsetResetPolicy()) {
                        log.info("{}, resetting offset", errorMessage);
                        subscriptions.requestOffsetResetIfPartitionAssigned(tp);
                    } else {
                        log.info("{}, raising error to the application since no reset policy is configured", errorMessage);
                        throw new OffsetOutOfRangeException(errorMessage,
                                Collections.singletonMap(tp, position.offset));
                    }
                }
            } else {
                log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                        clearedReplicaId.get(), tp, error, fetchOffset);
            }
        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
            //we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
            log.warn("Not authorized to read from partition {}.", tp);
            throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
        } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
            log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            log.warn("Unknown server error while fetching offset {} for topic-partition {}",
                    fetchOffset, tp);
        } else if (error == Errors.CORRUPT_MESSAGE) {
            throw new KafkaException("Encountered corrupt message when fetching offset "
                    + fetchOffset
                    + " for topic-partition "
                    + tp);
        } else {
            throw new IllegalStateException("Unexpected error code "
                    + error.code()
                    + " while fetching at offset "
                    + fetchOffset
                    + " from topic-partition " + tp);
        }
    }
}
