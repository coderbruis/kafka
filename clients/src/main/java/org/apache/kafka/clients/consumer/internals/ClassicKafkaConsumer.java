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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createConsumerNetworkClient;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * A client that consumes records from a Kafka cluster using the {@link GroupProtocol#CLASSIC classic group protocol}.
 * In this implementation, all network I/O happens in the thread of the application making the call.
 *
 * <p/>
 *
 * This {@link ConsumerDelegate} implementation exists for backward compatibility to allow users to continue to use
 * the classic group protocol (pre-KIP 848).
 *
 * <p/>
 *
 * <em>Note:</em> This class should not be invoked directly; users should instead create and use the
 * {@link KafkaConsumer} API as before.
 */
public class ClassicKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;
    public static final String DEFAULT_REASON = "rebalance enforced by user";

    private final Metrics metrics;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final Optional<String> groupId;
    private final ConsumerCoordinator coordinator;
    private final Deserializers<K, V> deserializers;
    private final FetchMetricsManager fetchMetricsManager;
    private final Fetcher<K, V> fetcher;
    private final OffsetFetcher offsetFetcher;
    private final TopicMetadataFetcher topicMetadataFetcher;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;

    private final Time time;
    private final ConsumerNetworkClient client;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private final int requestTimeoutMs;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;
    private final List<ConsumerPartitionAssignor> assignors;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    // currentThread holds the threadId of the current thread accessing this Consumer
    // and is used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;

    ClassicKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);
            if (groupRebalanceConfig.groupId != null && groupRebalanceConfig.groupId.isEmpty()) {
                throw new InvalidGroupIdException("The configured " + ConsumerConfig.GROUP_ID_CONFIG
                        + " should not be an empty string or whitespace.");
            }

            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            this.log = logContext.logger(getClass());
            boolean enableAutoCommit = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG);

            log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = Time.SYSTEM;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            this.metrics = createMetrics(config, time, reporters);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            this.retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList, metrics);
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer, metrics);
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    metrics.reporters(),
                    interceptorList,
                    Arrays.asList(this.deserializers.keyDeserializer(), this.deserializers.valueDeserializer()));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            this.metadata.bootstrap(addresses);

            this.fetchMetricsManager = createFetchMetricsManager(metrics);
            FetchConfig fetchConfig = new FetchConfig(config);
            this.isolationLevel = fetchConfig.isolationLevel;

            ApiVersions apiVersions = new ApiVersions();
            this.client = createConsumerNetworkClient(config,
                    metrics,
                    logContext,
                    apiVersions,
                    time,
                    metadata,
                    fetchMetricsManager.throttleTimeSensor(),
                    retryBackoffMs,
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // If the classic rebalance protocol is used, log message to guide users towards upgrading to the
            // next-generation consumer rebalance protocol
            if (groupId.isPresent()) {
                boolean isStreamsConsumer = assignors.stream()
                        .anyMatch(a -> a.getClass().getName().contains("StreamsPartitionAssignor"));
                if (!isStreamsConsumer) {
                    log.info("\n" +
                            "****************************************************************\n" +
                            "* The consumer rebalance protocol (KIP-848) is production-ready!\n" +
                            "* Set the consumer configuration {}={} to try it out.\n" +
                            "* See https://kafka.apache.org/documentation/#consumer_rebalance_protocol\n" +
                            "****************************************************************",
                            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
                }
            }

            // no coordinator will be constructed for the default (null) group id
            if (groupId.isEmpty()) {
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
                config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
                this.coordinator = null;
            } else {
                this.coordinator = new ConsumerCoordinator(groupRebalanceConfig,
                        logContext,
                        this.client,
                        assignors,
                        this.metadata,
                        this.subscriptions,
                        metrics,
                        CONSUMER_METRIC_GROUP_PREFIX,
                        this.time,
                        enableAutoCommit,
                        config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                        this.interceptors,
                        config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED),
                        clientTelemetryReporter);
            }
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    this.metadata,
                    this.subscriptions,
                    fetchConfig,
                    this.deserializers,
                    fetchMetricsManager,
                    this.time,
                    apiVersions);
            this.offsetFetcher = new OffsetFetcher(logContext,
                    client,
                    metadata,
                    subscriptions,
                    time,
                    retryBackoffMs,
                    requestTimeoutMs,
                    isolationLevel,
                    apiVersions);
            this.topicMetadataFetcher = new TopicMetadataFetcher(logContext,
                    client,
                    retryBackoffMs,
                    retryBackoffMaxMs);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics);

            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                // If a consumer fails during initialization, it means it hasn't joined the group yet.
                // Since it's not a group member, we use REMAIN_IN_GROUP option when closing
                // to prevent sending an unnecessary leave request to the coordinator.
                close(Duration.ZERO, CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // visible for testing
    ClassicKafkaConsumer(LogContext logContext,
                         Time time,
                         ConsumerConfig config,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer,
                         KafkaClient client,
                         SubscriptionState subscriptions,
                         ConsumerMetadata metadata,
                         List<ConsumerPartitionAssignor> assignors) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.metrics = new Metrics(time);
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        this.groupId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_ID_CONFIG));
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer, metrics);
        this.isolationLevel = ConsumerUtils.configuredIsolationLevel(config);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.assignors = assignors;
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics);
        this.interceptors = new ConsumerInterceptors<>(Collections.emptyList(), metrics);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientTelemetryReporter = Optional.empty();

        int sessionTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        int rebalanceTimeoutMs = config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        boolean enableAutoCommit = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG);
        boolean throwOnStableOffsetNotSupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        int autoCommitIntervalMs = config.getInt(AUTO_COMMIT_INTERVAL_MS_CONFIG);
        String rackId = config.getString(CLIENT_RACK_CONFIG);
        Optional<String> groupInstanceId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));

        this.client = new ConsumerNetworkClient(
            logContext,
            client,
            metadata,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            heartbeatIntervalMs
        );

        if (groupId.isPresent()) {
            GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(
                sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId.get(),
                groupInstanceId,
                rackId,
                retryBackoffMs,
                retryBackoffMaxMs
            );
            this.coordinator = new ConsumerCoordinator(
                rebalanceConfig,
                logContext,
                this.client,
                assignors,
                metadata,
                subscriptions,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                time,
                enableAutoCommit,
                autoCommitIntervalMs,
                interceptors,
                throwOnStableOffsetNotSupported,
                clientTelemetryReporter
            );
        } else {
            this.coordinator = null;
        }

        int maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        int maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        int minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        int fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        int maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        boolean checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG);

        FetchMetricsRegistry fetchMetricsRegistry = new FetchMetricsRegistry(CONSUMER_METRIC_GROUP_PREFIX);
        this.fetchMetricsManager = new FetchMetricsManager(metrics, fetchMetricsRegistry);
        ApiVersions apiVersions = new ApiVersions();
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                checkCrcs,
                rackId,
                isolationLevel
        );
        this.fetcher = new Fetcher<>(
            logContext,
            this.client,
            metadata,
            subscriptions,
            fetchConfig,
            deserializers,
            fetchMetricsManager,
            time,
            apiVersions
        );
        this.offsetFetcher = new OffsetFetcher(
            logContext,
            this.client,
            metadata,
            subscriptions,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            isolationLevel,
            apiVersions
        );
        this.topicMetadataFetcher = new TopicMetadataFetcher(
            logContext,
            this.client,
            retryBackoffMs,
            retryBackoffMaxMs
        );
    }

    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Set.copyOf(this.subscriptions.subscription());
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(topics, Optional.of(listener));
    }


    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        } else {
            log.debug("Skipping registration for metric {}. Existing consumer metrics cannot be overwritten.", metric.metricName());
        }
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        }  else {
            log.debug("Skipping unregistration for metric {}. Existing consumer metrics cannot be removed.", metric.metricName());
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribeInternal(topics, Optional.empty());
    }

    /**
     * Internal helper method for {@link #subscribe(Collection)} and
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}
     * <p>
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * @param topics The list of topics to subscribe to
     * @param listener {@link Optional} listener instance to get notifications on partition assignment/revocation
     *                 for the subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    private void subscribeInternal(Collection<String> topics, Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (isBlank(topic))
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }

                throwIfNoAssignorsConfigured();

                // Clear the buffered data which are not a part of newly assigned topics
                final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

                for (TopicPartition tp : subscriptions.assignedPartitions()) {
                    if (topics.contains(tp.topic()))
                        currentTopicPartitions.add(tp);
                }

                fetcher.clearBufferedDataForUnassignedPartitions(currentTopicPartitions);

                log.info("Subscribed to topic(s): {}", String.join(", ", topics));
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(pattern, Optional.of(listener));
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribeInternal(pattern, Optional.empty());
    }

    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException(String.format("Subscribe to RE2/J pattern is not supported when using" +
            "the %s protocol defined in config %s", GroupProtocol.CLASSIC, ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    @Override
    public void subscribe(SubscriptionPattern pattern) {
        throw new UnsupportedOperationException(String.format("Subscribe to RE2/J pattern is not supported when using" +
            "the %s protocol defined in config %s", GroupProtocol.CLASSIC, ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    /**
     * Internal helper method for {@link #subscribe(Pattern)} and
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}
     * <p>
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     *
     * @param pattern Pattern to subscribe to
     * @param listener {@link Optional} listener instance to get notifications on partition assignment/revocation
     *                 for the subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    private void subscribeInternal(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        throwIfGroupIdNotDefined();
        if (pattern == null || pattern.toString().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        acquireAndEnsureOpen();
        try {
            throwIfNoAssignorsConfigured();
            log.info("Subscribed to pattern: '{}'", pattern);
            this.subscriptions.subscribe(pattern, listener);
            this.coordinator.updatePatternSubscription(metadata.fetch());
            this.metadata.requestUpdateForNewTopics();
        } finally {
            release();
        }
    }

    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
            if (this.coordinator != null) {
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup(CloseOptions.GroupMembershipOperation.DEFAULT, "the consumer unsubscribed from all topics");
            }
            this.subscriptions.unsubscribe();
            log.info("Unsubscribed all topics or patterns and assigned partitions");
        } finally {
            release();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                this.unsubscribe();
            } else {
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (isBlank(topic))
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                }
                fetcher.clearBufferedDataForUnassignedPartitions(partitions);

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                if (coordinator != null)
                    this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

                log.info("Assigned to partition(s): {}", partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
                if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout));
    }

    /**
     * 在用户给定的超时时间内，完成消费组协调、分区位置准备、发送/接收 fetch 请求，并把拉到的消息返回给用户。
     * @throws KafkaException if the rebalance callback throws exception
     */
    private ConsumerRecords<K, V> poll(final Timer timer) {
        acquireAndEnsureOpen();
        try {
            // 统计同一线程两次 poll() 调用之间隔了多久
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            // 消费者必须订阅主题，并且自动分区或者手动指定分区
            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            // 在超时时间范围内，只要没拿到数据就持续多轮poll
            do {
                // 在可能发生阻塞操作前响应用户通过 wakeup() 发出的唤醒请求。
                client.maybeTriggerWakeup();

                // try to update assignment metadata BUT do not need to block on the timer for join group
                // 在拉取消息前，先确保消费者已经完成入组和分区分配，并准备好各分区接下来应该从哪个 offset 开始消费。
                updateAssignmentMetadataIfNeeded(timer, false);
                // 尝试从 fetch buffer 取数据；如果没有，就发送 fetch 请求并等待 broker 响应。
                final Fetch<K, V> fetch = pollForFetches(timer);
                // 如果本轮已经拿到可返回的数据，准备返回给用户
                if (!fetch.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    // 提前发送下一轮 fetch 请求。用户处理当前 records 的时候，下一批数据已经在路上，提高吞吐，形成 pipeline。
                    if (sendFetches() > 0 || client.hasPendingRequests()) {
                        client.transmitSends();
                    }

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }
                    // 把拉到的数据交给 consumer interceptor 处理，然后包装成 ConsumerRecords 返回给用户。
                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records(), fetch.nextOffsets()));
                }
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            // 释放 consumer 使用权
            release();
            // 记录 poll 结束指标
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    private int sendFetches() {
        offsetFetcher.validatePositionsOnMetadataChange();
        return fetcher.sendFetches();
    }

    /**
     * 在拉取消息前，先确保消费者已经完成入组和分区分配，并准备好各分区接下来应该从哪个 offset 开始消费。
     * @param timer
     * @param waitForJoinGroup true：在这里等入组完成。。false：先发起或推进入组，没完成就先往下走，后续循环继续。
     * @return
     */
    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        // 判断消费者组协调是否完成（加入消费者组、重平衡、心跳协调）
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        // 准备各分区的拉取位置，即consumer该从哪个offset开始拉消息
        return updateFetchPositions(timer);
    }

    /**
     * 先从本地已完成的 fetch 结果里取数据；没有数据就发送新的 fetch 请求；再通过 client.poll() 推进网络 I/O，等待 broker 响应；最后再取一次数据返回。
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
        // 计算这次最多阻塞多久。
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        // if data is available already, return it immediately
        // 先看本地是否已经有可返回的数据。
        final Fetch<K, V> fetch = fetcher.collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        // fetchBuffer中没有数据，发起fetch请求
        sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        // 如果当前订阅的分区还不是全部都有有效 fetch position，就不要长时间阻塞等 fetch。
        // 因为 position 不完整时，可能还在查 committed offset、reset offset、校验 leader epoch，或者刚失败进入 backoff。
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            // 这个时候等太久没意义，所以最多等 retryBackoffMs，尽快回到外层循环继续推进 position 准备流程。
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        // client.poll(...) 会推进 Kafka 网络 I/O：发送请求、接收响应、执行回调。
        client.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            // 这个匿名参数是继续阻塞的条件：只要 fetcher 还没有可用 fetch，就可以继续等；一旦有 fetch 响应完成并进入可取状态，就提前结束等待，不必把 pollTimeout 用完。
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());
        // fetch请求之后，再从fetcher收集一次数据。
        return fetcher.collectFetch();
    }

    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            throwIfGroupIdNotDefined();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            if (!coordinator.commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            release();
        }
    }

    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            log.debug("Committing offsets: {}", offsets);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offset,
                    Optional.empty(), // This will ensure we skip validation
                    this.metadata.currentLeader(partition));
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                        offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(),
                    currentLeaderAndEpoch);
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, AutoOffsetResetStrategy.EARLIEST);
        } finally {
            release();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, AutoOffsetResetStrategy.LATEST);
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            Timer timer = time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
                if (position != null)
                    return position.offset;

                updateFetchPositions(timer);
                client.poll(timer);
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                    "for partition " + partition + " could be determined");
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        acquireAndEnsureOpen();
        long start = time.nanoseconds();
        try {
            throwIfGroupIdNotDefined();
            final Map<TopicPartition, OffsetAndMetadata> offsets;
            offsets = coordinator.fetchCommittedOffsets(partitions, time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                        "committed offset for partitions " + partitions + " could be determined. Try tuning " +
                        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            release();
        }
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");

        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            Timer timer = time.timer(timeout);
            List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topic, metadata.allowAutoTopicCreation(), timer);
            return topicMetadata != null ? topicMetadata : Collections.emptyList();
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return topicMetadataFetcher.getAllTopicMetadata(time.timer(timeout));
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition: partitions) {
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Resuming partitions {}", partitions);
            for (TopicPartition partition: partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
                // OffsetAndTimestamp is always positive.
                if (entry.getValue() < 0)
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                            entry.getValue() + ". The target time cannot be negative.");
            }
            return offsetFetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.beginningOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.endOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.currentLag(topicPartition);
        } finally {
            release();
        }
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            return coordinator.groupMetadata();
        } finally {
            release();
        }
    }

    @Override
    public void enforceRebalance(final String reason) {
        acquireAndEnsureOpen();
        try {
            if (coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            coordinator.requestRejoin(reason == null || reason.isEmpty() ? DEFAULT_REASON : reason);
        } finally {
            release();
        }
    }

    @Override
    public void enforceRebalance() {
        enforceRebalance(null);
    }

    @Override
    public void close() {
        close(CloseOptions.timeout(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS)));
    }

    @Deprecated
    @Override
    public void close(Duration timeout) {
        close(CloseOptions.timeout(timeout));
    }

    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    @Override
    public void close(CloseOptions option) {
        Duration timeout = option.timeout().orElseGet(() -> Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, option.groupMembershipOperation(), false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    private Timer createTimerForRequest(final Duration timeout) {
        // this.time could be null if an exception occurs in constructor prior to setting the this.time field
        final Time localTime = (time == null) ? Time.SYSTEM : time;
        return localTime.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    private void close(Duration timeout, CloseOptions.GroupMembershipOperation membershipOperation, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        final Timer closeTimer = createTimerForRequest(timeout);
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        closeTimer.update();
        // Close objects with a timeout. The timeout is required because the coordinator & the fetcher send requests to
        // the server in the process of closing which may not respect the overall timeout defined for closing the
        // consumer.
        if (coordinator != null) {
            // This is a blocking call bound by the time remaining in closeTimer
            swallow(
                log,
                Level.ERROR,
                "Failed to close coordinator with a timeout(ms)=" + closeTimer.timeoutMs(),
                () -> coordinator.close(closeTimer, membershipOperation),
                firstException
            );
        }

        if (fetcher != null) {
            // the timeout for the session close is at-most the requestTimeoutMs
            long remainingDurationInTimeout = Math.max(0, timeout.toMillis() - closeTimer.elapsedMs());
            if (remainingDurationInTimeout > 0) {
                remainingDurationInTimeout = Math.min(requestTimeoutMs, remainingDurationInTimeout);
            }

            closeTimer.reset(remainingDurationInTimeout);

            // This is a blocking call bound by the time remaining in closeTimer
            swallow(log, Level.ERROR, "Failed to close fetcher with a timeout(ms)=" + closeTimer.timeoutMs(), () -> fetcher.close(closeTimer), firstException);
        }

        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(fetchMetricsManager, "kafka fetch metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(client, "consumer network client", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
        clientTelemetryReporter.ifPresent(reporter -> closeQuietly(reporter, "consumer telemetry reporter", firstException));
        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    /**
     * 确保当前分配到的每个分区，都有一个明确的“下一次从哪里开始拉消息”的位置。
     * 这个位置可能来自：
     * 1)当前已经保存好的 fetch position。
     * 2)已提交的 offset。
     * 3)auto.offset.reset 策略，比如 earliest / latest。
     * 4)手动 seek() 设置的位置。
     * 如果在超时时间内拿不到需要的已提交 offset，就返回 false。如果缺少 offset 且没有配置 reset 策略，就会抛 NoOffsetForPartitionException。
     * <p>
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     * @return true iff the operation completed without timing out
     */
    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        // 检查已有的 fetch position 是否还有效。
        offsetFetcher.validatePositionsIfNeeded();

        // 检查当前订阅状态里，所有已分配分区是否都有有效的 fetch position。
        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        // 如果所有分区都有位置了，就不需要再查 committed offset，也不需要 reset，直接返回成功。
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        // 如果存在 consumer coordinator，就尝试为“还没有 fetch position 的分区”读取已提交 offset。也就是问 coordinator：这些分区之前消费到哪里了？
        // 如果在 timer 时间内没有查到 committed offset，就返回 false，本轮先不继续。这里 coordinator != null 是因为：
        // 1)自动分配分区时通常有 coordinator。
        // 2)手动 assign() 的消费者也可能不依赖 coordinator。
        // 3)如果没有 coordinator，就跳过 committed offset 初始化，后面走 reset 策略。
        if (coordinator != null && !coordinator.initWithCommittedOffsetsIfNeeded(timer)) return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        // 对仍然没有 fetch position 的分区，按默认 offset reset 策略准备重置位置。
        // 比如：
        // 1)earliest：准备从最早 offset 开始。
        // 2)latest：准备从最新 offset 开始。
        // 3)none：没有可用 offset 时直接抛 NoOffsetForPartitionException。
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to look up and update the positions of any
        // partitions which are awaiting reset.
        // 对刚才标记为需要 reset 的分区，异步发送 ListOffsets 请求，去 broker 查询真正的 earliest/latest offset，并更新 fetch position。
        offsetFetcher.resetPositionsIfNeeded();

        return true;
    }

    /**
     * KafkaConsumer 不是线程安全的。进入 poll 前获取使用权，并确认消费者尚未关闭。
     * 在kafka中除了wakeup()和close()，只要读取或者修改Consumer内部状态的公开方法都需要调acquireAndEnsureOpen()。
     * <p>
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * 检测并阻止多个线程同时使用同一个 KafkaConsumer。允许同一线程重入，refcount计算重入次数。
     * <p>
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
     * supported).
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                    "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                    " otherThread(id: " + currentThread.get() + ")"
            );
        // 重入次数计算
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    private void throwIfGroupIdNotDefined() {
        if (groupId.isEmpty())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    // Functions below are for testing only
    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public Metrics metricsRegistry() {
        return metrics;
    }

    @Override
    public KafkaConsumerMetrics kafkaConsumerMetrics() {
        return kafkaConsumerMetrics;
    }

    @Override
    public boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }
}
