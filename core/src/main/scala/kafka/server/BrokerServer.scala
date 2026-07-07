/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.coordinator.group.CoordinatorPartitionWriter
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.network.SocketServer
import kafka.raft.KafkaRaftManager
import kafka.server.metadata._
import kafka.server.share.{ReplicaManagerLogReader, ReplicaManagerPartitionMetadataProvider, ShareCoordinatorMetadataCacheHelperImpl, SharePartitionManager}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.utils.internals.LogContext
import org.apache.kafka.common.{ClusterResource, TopicPartition, Uuid}
import org.apache.kafka.coordinator.common.runtime.{CoordinatorLoaderImpl, CoordinatorRecord}
import org.apache.kafka.coordinator.group.metrics.{GroupCoordinatorMetrics, GroupCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.group.{GroupConfigManager, GroupCoordinator, GroupCoordinatorRecordSerde, GroupCoordinatorService}
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigProvider
import org.apache.kafka.coordinator.share.metrics.{ShareCoordinatorMetrics, ShareCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorRecordSerde, ShareCoordinatorService}
import org.apache.kafka.coordinator.transaction.ProducerIdManager
import org.apache.kafka.image.publisher.{BrokerRegistrationTracker, MetadataPublisher}
import org.apache.kafka.metadata.{BrokerState, KRaftMetadataCache, ListenerInfo, MetadataCache, MetadataVersionConfigValidator}
import org.apache.kafka.metadata.publisher.{AclPublisher, DelegationTokenPublisher, DynamicClientQuotaPublisher, DynamicTopicClusterQuotaPublisher, ScramPublisher}
import org.apache.kafka.security.{CredentialProvider, DelegationTokenManager}
import org.apache.kafka.server.FetchSession.FetchSessionCache
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{ApiMessageAndVersion, DirectoryEventHandler, NodeToControllerChannelManager, ShareVersion, TopicIdPartition}
import org.apache.kafka.server.config.{ConfigType, DelegationTokenManagerConfigs}
import org.apache.kafka.server.log.remote.metadata.storage.BrokerReadyCallback
import org.apache.kafka.server.log.remote.storage.{RemoteLogManager, RemoteLogManagerConfig}
import org.apache.kafka.server.metrics.{ClientTelemetryExporterPlugin, KafkaYammerMetrics}
import org.apache.kafka.server.network.{EndpointReadyFutures, KafkaAuthorizerServerInfo}
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey
import org.apache.kafka.server.share.persister.{DefaultStatePersister, NoOpStatePersister, Persister}
import org.apache.kafka.server.share.session.ShareSessionCache
import org.apache.kafka.server.util.timer.{SystemTimer, SystemTimerReaper, Timer}
import org.apache.kafka.server.util.{Deadline, FutureUtils, KafkaScheduler, NetworkPartitionMetadataClient, PartitionMetadataClient}
import org.apache.kafka.server.{AssignmentsManager, AutoTopicCreationManager, BrokerFeatures, BrokerLifecycleManager, ClientMetricsManager, DefaultApiVersionManager, DefaultAutoTopicCreationManager, DelayedActionQueue, FetchManager, FetchSessionCacheShard, ForwardingManager, ForwardingManagerImpl, KRaftTopicCreator, NodeToControllerChannelManagerImpl, ProcessRole, RaftControllerNodeProvider}
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager
import org.apache.kafka.storage.internals.log.{LogDirFailureChannel, LogManager => JLogManager}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.apache.kafka.server.partition.{AlterPartitionManager, DefaultAlterPartitionManager}
import org.apache.kafka.server.share.dlq.{DefaultShareGroupDLQManager, NoOpShareGroupDLQManager, ShareGroupDLQManager}
import org.apache.kafka.server.share.metrics.ShareGroupMetrics

import java.time.Duration
import java.util
import java.util.Optional
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}
import scala.collection.Map
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption


/**
 * A Kafka broker that runs in KRaft (Kafka Raft) mode.
 */
class BrokerServer(
  val sharedServer: SharedServer
) extends KafkaBroker {
  val config: KafkaConfig = sharedServer.brokerConfig
  val time: Time = sharedServer.time
  def metrics: Metrics = sharedServer.metrics

  // Get raftManager from SharedServer. It will be initialized during startup.
  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = sharedServer.raftManager

  override def brokerState: BrokerState = Option(lifecycleManager).
    flatMap(m => Some(m.state)).getOrElse(BrokerState.NOT_RUNNING)

  import kafka.server.Server._

  private val logContext: LogContext = new LogContext(s"[BrokerServer id=${config.nodeId}] ")

  this.logIdent = logContext.logPrefix

  @volatile var lifecycleManager: BrokerLifecycleManager = _

  private var assignmentsManager: AssignmentsManager = _

  private var partitionMetadataClient: PartitionMetadataClient = _

  val lock: ReentrantLock = new ReentrantLock()
  val awaitShutdownCond: Condition = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  @volatile var dataPlaneRequestProcessor: KafkaApis = _

  var authorizerPlugin: Option[Plugin[Authorizer]] = None
  @volatile var socketServer: SocketServer = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _

  var logDirFailureChannel: LogDirFailureChannel = _
  var logManager: JLogManager = _
  var remoteLogManagerOpt: Option[RemoteLogManager] = None

  var tokenManager: DelegationTokenManager = _

  var dynamicConfigHandlers: Map[ConfigType, ConfigHandler] = _

  @volatile private[this] var _replicaManager: ReplicaManager = _

  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _

  @volatile var groupCoordinator: GroupCoordinator = _

  var groupConfigManager: GroupConfigManager = _

  var transactionCoordinator: TransactionCoordinator = _

  var shareCoordinator: ShareCoordinator = _

  var clientToControllerChannelManager: NodeToControllerChannelManager = _

  var forwardingManager: ForwardingManager = _

  var alterPartitionManager: AlterPartitionManager = _

  var autoTopicCreationManager: AutoTopicCreationManager = _

  var kafkaScheduler: KafkaScheduler = _

  @volatile var metadataCache: KRaftMetadataCache = _

  var quotaManagers: QuotaFactory.QuotaManagers = _

  var clientQuotaMetadataManager: ClientQuotaMetadataManager = _

  @volatile var brokerTopicStats: BrokerTopicStats = _

  val clusterId: String = sharedServer.metaPropsEnsemble.clusterId().get()

  var brokerMetadataPublisher: BrokerMetadataPublisher = _

  var brokerRegistrationTracker: BrokerRegistrationTracker = _

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault(config.unstableFeatureVersionsEnabled)

  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()

  var clientMetricsManager: ClientMetricsManager = _

  var shareGroupMetrics: ShareGroupMetrics = _

  var sharePartitionManager: SharePartitionManager = _

  var persister: Persister = _

  private var shareGroupTimer: Timer = _

  private var shareGroupDLQManager: ShareGroupDLQManager = _

  private var shareGroupLogReader: ReplicaManagerLogReader = _

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      info(s"Transition from $status to $to")

      status = to
      if (to == SHUTDOWN) {
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  def replicaManager: ReplicaManager = _replicaManager

  override def startup(): Unit = {
    // 启动状态保护和超时控制。防止重复启动。
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS)
    try {
      // 启动 broker/controller 共享的 KRaft 基础设施。
      // 为了先把 KRaft metadata log 读取、metadata loader、snapshot 机制 这些公共底座启动起来。
      // 没有它，broker 后面就拿不到集群元数据，也无法完成 registration / catch-up / unfence。
      sharedServer.startForBroker()

      // 记录 broker 开始启动，便于启动日志定位。
      info("Starting broker")

      // 创建客户端遥测导出插件，供动态配置和 ClientMetricsManager 使用。
      val clientTelemetryExporterPlugin = new ClientTelemetryExporterPlugin()

      // 初始化动态配置，clientTelemetry是用于客户端指标观测，不直接参与 Produce/Fetch 的核心读写逻辑。
      config.dynamicConfig.initialize(Some(clientTelemetryExporterPlugin))
      // 实例化quota，quota 是 broker 的资源保护机制，用来限制客户端、用户或操作类型的资源使用，防止单个调用方或某类请求拖垮 broker。
      quotaManagers = QuotaFactory.instantiate(config, metrics, time, s"broker-${config.nodeId}-", ProcessRole.BrokerRole.toString)
      // 并从 metadata snapshot 读取动态 broker 配置
      DynamicBrokerConfig.readDynamicBrokerConfigsFromSnapshot(raftManager, config, quotaManagers, logContext)

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      // 启动后台调度器，这个是broker 的通用后台任务线程池，后面 LogManager、ReplicaManager 等组件创建后，就可以往里面注册周期任务。
      kafkaScheduler.startup()

      /* register broker metrics */
      // Kafka broker 上 topic 级别 + broker 汇总级别的吞吐、请求、失败、复制、remote storage 指标统计器
      brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled())
      // 日志目录故障通知通道。并不是恢复组件，只是“发现日志目录坏了之后，用来统一上报和排队通知”的通道
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)
      // metadataCache 是 broker 本地保存的 KRaft 集群元数据快照，供 broker 各组件快速判断 topic、partition、broker、leader、配置和 feature 状态。
      metadataCache = new KRaftMetadataCache(config.nodeId, () => raftManager.client.kraftVersion())

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      // 负责本地日志目录、分区日志、恢复、flush、retention 等。但这里先创建，不急着完整恢复，因为要等 metadata catch-up 后才知道最新 topic/broker 配置。
      logManager = LogManager(config,
        sharedServer.metaPropsEnsemble.errorLogDirs(),
        metadataCache,
        kafkaScheduler,
        time,
        brokerTopicStats,
        logDirFailureChannel)
      // 负责 broker 注册、心跳、broker epoch、等待 controller 确认 catch-up、unfence 等生命周期。
      lifecycleManager = new BrokerLifecycleManager(
        config,
        time,
        s"broker-${config.nodeId}-",
        logManager.directoryIds,
        () => new Thread(() => shutdown(), "kafka-shutdown-thread").start(),
        () => metadataCache.metadataVersion().isCordonedLogDirsSupported)

      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      // 给认证、SCRAM、delegation token 动态更新使用。
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      // 等待 sharedServer.controllerQuorumVotersFuture 完成。
      // waitWithLogging 的作用是等待 future，同时带日志，方便定位卡在哪一步。
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time)
      // 创建一个 controllerNodeProvider。它负责告诉 broker：当前应该连接哪个 controller 节点。
      val controllerNodeProvider = RaftControllerNodeProvider.create(raftManager, config)
      // 用来转发需要 controller 处理的请求；forwardingManager 是 Kafka API 层使用的封装。
      clientToControllerChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "forwarding",
        s"broker-${config.nodeId}-",
        60000
      )
      // 启动 broker 到 controller 的网络通道。
      clientToControllerChannelManager.start()
      // 封装转发到 controller 的能力，供 KafkaApis 处理控制类请求。
      forwardingManager = new ForwardingManagerImpl(clientToControllerChannelManager, metrics)
      // 管理客户端遥测指标和连接级指标状态。
      clientMetricsManager = new ClientMetricsManager(clientTelemetryExporterPlugin, config.clientTelemetryMaxBytes, time, metrics)

      // 管理 broker listener 支持的 API 版本和 feature 信息。
      val apiVersionManager = new DefaultApiVersionManager(
        ListenerType.BROKER,
        () => forwardingManager.controllerApiVersions,
        brokerFeatures,
        metadataCache,
        config.unstableApiVersionsEnabled,
        Optional.of(clientMetricsManager)
      )

      // 缓存 share fetch 会话，减少 share group fetch 请求开销。
      val shareFetchSessionCache : ShareSessionCache = new ShareSessionCache(
        config.shareGroupConfig.shareGroupMaxShareSessions()
      )

      // 注册连接断开回调，用于清理客户端指标和 share fetch 会话。
      val connectionDisconnectListeners = Seq(
        clientMetricsManager.connectionDisconnectListener(),
        shareFetchSessionCache.connectionDisconnectListener()
      )

      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      // 创建网络入口。此时准备 acceptor 和端口，真正处理请求会延后开启。
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager,
        sharedServer.socketFactory,
        connectionDisconnectListeners)

      // 连接 quota 元数据管理器和 SocketServer 的连接 quota。
      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas)

      // 构造 broker 对外注册的 listener 信息，并修正通配地址和临时端口。
      val listenerInfo = ListenerInfo.create(Optional.of(config.interBrokerListenerName.value()),
          config.effectiveAdvertisedBrokerListeners.asJava).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(name => socketServer.boundPort(new ListenerName(name)))

      // 如果启用 tiered storage，创建 remote log manager。
      remoteLogManagerOpt = createRemoteLogManager(listenerInfo)

      // 创建 AlterPartitionManager，用于向 controller 提交分区状态变更。
      alterPartitionManager = DefaultAlterPartitionManager.create(
        config,
        kafkaScheduler,
        controllerNodeProvider,
        time,
        metrics,
        s"broker-${config.nodeId}-",
        () => lifecycleManager.brokerEpoch
      )
      // 启动分区状态变更管理器。
      alterPartitionManager.start()

      // 创建事务 AddPartitionsToTxn 组件使用的日志上下文和网络客户端。
      val addPartitionsLogContext = new LogContext(s"[AddPartitionsToTxnManager broker=${config.brokerId}]")
      val addPartitionsToTxnNetworkClient = NetworkUtils.buildNetworkClient("AddPartitionsManager", config, metrics, time, addPartitionsLogContext)
      // 管理事务中新增分区的 controller 交互。
      val addPartitionsToTxnManager = new AddPartitionsToTxnManager(
        config,
        addPartitionsToTxnNetworkClient,
        metadataCache,
        // The transaction coordinator is not created at this point so we must
        // use a lambda here.
        transactionalId => transactionCoordinator.partitionFor(transactionalId),
        time
      )

      // 创建目录分配到 controller 的通信通道。
      val assignmentsChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "directory-assignments",
        s"broker-${config.nodeId}-",
        60000
      )
      // 管理 KRaft/JBOD 下 partition 到 log directory 的分配。
      assignmentsManager = new AssignmentsManager(
        time,
        assignmentsChannelManager,
        config.brokerId,
        () => metadataCache.getImage(),
        (directoryId: Uuid) => logManager.directoryPath(directoryId).
          orElse("[unknown directory path]")
      )
      // 把目录分配、失败、cordon 事件转交给对应生命周期组件。
      val directoryEventHandler = new DirectoryEventHandler {
        override def handleAssignment(partition: TopicIdPartition, directoryId: Uuid, reason: String, callback: Runnable): Unit =
          assignmentsManager.onAssignment(partition, directoryId, reason, callback)

        override def handleFailure(directoryId: Uuid): Unit =
          lifecycleManager.propagateDirectoryFailure(directoryId, config.logDirFailureTimeoutMs)

        override def handleCordoned(directoryIds: util.Set[Uuid]): Unit =
          lifecycleManager.propagateDirectoryCordoned(directoryIds)
}

      /**
       * TODO: move this action queue to handle thread so we can simplify concurrency handling
       */
      // ReplicaManager 使用的默认延迟动作队列。
      val defaultActionQueue = new DelayedActionQueue

      // 创建 ReplicaManager，负责副本、分区状态、Produce/Fetch 落盘等数据面核心逻辑。
      this._replicaManager = new ReplicaManager(
        config = config,
        metrics = metrics,
        time = time,
        scheduler = kafkaScheduler,
        logManager = logManager,
        remoteLogManager = remoteLogManagerOpt,
        quotaManagers = quotaManagers,
        metadataCache = metadataCache,
        logDirFailureChannel = logDirFailureChannel,
        alterPartitionManager = alterPartitionManager,
        brokerTopicStats = brokerTopicStats,
        delayedRemoteFetchPurgatoryParam = None,
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch,
        addPartitionsToTxnManager = Some(addPartitionsToTxnManager),
        directoryEventHandler = directoryEventHandler,
        defaultActionQueue = defaultActionQueue
      )

      /* start token manager */
      // 启动 delegation token 管理能力。
      tokenManager = new DelegationTokenManager(new DelegationTokenManagerConfigs(config), tokenCache)

      // Create and initialize an authorizer if one is configured.
      authorizerPlugin = config.createNewAuthorizer(metrics, ProcessRole.BrokerRole.toString)

      /* initializing the groupConfigManager */
      // 管理 group/share group 的动态配置视图。
      groupConfigManager = new GroupConfigManager(config.groupCoordinatorConfig, config.shareGroupConfig)

      /* create share coordinator */
      // 创建 share group coordinator。
      shareCoordinator = createShareCoordinator()

      /* create shared timer for share group components */
      // 创建 share group 组件共享的定时器。
      shareGroupTimer = new SystemTimerReaper("share-group-reaper", new SystemTimer("share-group"))

      /* create persister */
      // 创建 share group 状态持久化组件。
      persister = createShareStatePersister()

      /* create metrics object to be shared with share DLQ manager share partition manager*/
      // 创建 share group 指标对象。
      shareGroupMetrics = new ShareGroupMetrics(time)

      /* create log reader object to share with share group DLQ manager and SharePartitionManager */
      shareGroupLogReader = new ReplicaManagerLogReader(replicaManager)

      /* create share group DLQ manager */
      // 创建 share group DLQ 管理器。
      shareGroupDLQManager = createShareGroupDLQManager()

      // 创建分区元数据客户端，供 group/share 等组件查询分区信息。
      partitionMetadataClient = createPartitionMetadataClient(metadataCache)

      // 创建 group coordinator，负责 consumer group 协调。
      groupCoordinator = createGroupCoordinator()

      // 创建 producer id 管理器供应器，通过 controller 分配 producer id。
      val producerIdManagerSupplier = () => ProducerIdManager.rpc(
        config.brokerId,
        time,
        () => lifecycleManager.brokerEpoch,
        clientToControllerChannelManager
      )

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      // 创建事务协调器，让当前 broker 具备处理事务 producer 请求和维护事务状态的能力。
      // 管理transactional.id，分配、维护 producerId和producer epoch，维护事务状态机
      transactionCoordinator = TransactionCoordinator(config, replicaManager,
        new KafkaScheduler(1, true, "transaction-log-manager-"),
        producerIdManagerSupplier, metrics, metadataCache, Time.SYSTEM)

      // 创建自动建 topic 管理器，封装 group/txn/share 内部 topic 配置。
      autoTopicCreationManager = new DefaultAutoTopicCreationManager(
        config,
        () => groupCoordinator.groupMetadataTopicConfigs,
        () => transactionCoordinator.transactionStateTopicConfigs,
        () => shareCoordinator.shareGroupStateTopicConfigs,
        new KRaftTopicCreator(clientToControllerChannelManager),
        time,
      )

      // 注册各类动态配置变更处理器。
      dynamicConfigHandlers = Map[ConfigType, ConfigHandler](
        ConfigType.TOPIC -> new TopicConfigHandler(replicaManager, config, quotaManagers),
        ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers),
        ConfigType.CLIENT_METRICS -> new ClientMetricsConfigHandler(clientMetricsManager),
        ConfigType.GROUP -> new GroupConfigHandler(groupCoordinator))

      // 将 broker 支持的 feature 转成注册到 controller 的格式。
      val featuresRemapped = BrokerFeatures.createDefaultFeatureMap(brokerFeatures)

      // 创建 broker 心跳通道，用于生命周期管理器和 controller 通信。
      val brokerLifecycleChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "heartbeat",
        s"broker-${config.nodeId}-",
        config.brokerHeartbeatIntervalMs
      )
      // 启动 broker 注册/心跳/catch-up 生命周期流程。
      lifecycleManager.start(
        () => sharedServer.loader.lastAppliedOffset(),
        brokerLifecycleChannelManager,
        clusterId,
        listenerInfo.toBrokerRegistrationRequest,
        featuresRemapped,
        logManager.readBrokerEpochFromCleanShutdownFiles()
      )

      // The FetchSessionCache is divided into config.numIoThreads shards, each responsible
      // for Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange
      // 为 incremental fetch session 切分 session id 范围。
      val sessionIdRange = Int.MaxValue / NumFetchSessionCacheShards
      // 创建 fetch session cache 分片集合。
      val fetchSessionCacheShards: util.List[FetchSessionCacheShard] = new util.ArrayList()
      // 初始化每个 fetch session cache 分片。
      for (shardNum <- 0 until NumFetchSessionCacheShards) {
        fetchSessionCacheShards.add(new FetchSessionCacheShard(
          config.maxIncrementalFetchSessionCacheSlots / NumFetchSessionCacheShards,
          KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS,
          sessionIdRange,
          shardNum
        ))
      }
      // 创建 FetchManager，管理 fetch session 缓存。
      val fetchManager = new FetchManager(Time.SYSTEM, new FetchSessionCache(fetchSessionCacheShards))

      // 创建 share partition manager，负责 share group 分区读取、锁和状态管理。
      sharePartitionManager = new SharePartitionManager(
        replicaManager,
        shareGroupLogReader,
        new ReplicaManagerPartitionMetadataProvider(replicaManager),
        (key: DelayedShareFetchKey) => replicaManager.completeDelayedShareFetchRequest(key),
        time,
        shareFetchSessionCache,
        config.shareGroupConfig.shareGroupRecordLockDurationMs,
        config.shareGroupConfig.shareGroupDeliveryCountLimit,
        config.shareGroupConfig.shareGroupPartitionMaxRecordLocks,
        config.remoteLogManagerConfig.remoteFetchMaxWaitMs().toLong,
        persister,
        new ShareGroupConfigProvider(groupConfigManager),
        shareGroupMetrics,
        brokerTopicStats,
        () => ShareVersion.fromFeatureLevel(metadataCache.features.finalizedFeatures.getOrDefault(ShareVersion.FEATURE_NAME, 0.toShort)).supportsShareGroupDLQ(),
        shareGroupDLQManager
      )

      // 创建 KafkaApis，作为 broker 数据面请求的核心分发处理器。
      dataPlaneRequestProcessor = new KafkaApis(
        requestChannel = socketServer.dataPlaneRequestChannel,
        forwardingManager = forwardingManager,
        replicaManager = replicaManager,
        groupCoordinator = groupCoordinator,
        txnCoordinator = transactionCoordinator,
        shareCoordinator = shareCoordinator,
        autoTopicCreationManager = autoTopicCreationManager,
        brokerId = config.nodeId,
        config = config,
        configRepository = metadataCache,
        metadataCache = metadataCache,
        metrics = metrics,
        authorizerPlugin = authorizerPlugin,
        quotas = quotaManagers,
        fetchManager = fetchManager,
        sharePartitionManager = sharePartitionManager,
        brokerTopicStats = brokerTopicStats,
        clusterId = clusterId,
        time = time,
        tokenManager = tokenManager,
        apiVersionManager = apiVersionManager,
        clientMetricsManager = clientMetricsManager,
        groupConfigManager = groupConfigManager)

      // 创建请求处理线程池，从 RequestChannel 拉取请求并调用 KafkaApis。
      dataPlaneRequestHandlerPool = sharedServer.requestHandlerPoolFactory.createPool(
        config.nodeId,
        socketServer.dataPlaneRequestChannel,
        dataPlaneRequestProcessor,
        time,
        config.numIoThreads,
        "broker"
      )

      // 添加 metadata version 校验器，防止不兼容配置被发布。
      metadataPublishers.add(new MetadataVersionConfigValidator(config.brokerId,
        () => config.processRoles.contains(ProcessRole.BrokerRole) && config.logDirs().size() > 1,
        sharedServer.metadataPublishingFaultHandler
      ))
      // 创建 broker metadata publisher，把 KRaft 元数据发布到 broker 本地组件。
      brokerMetadataPublisher = new BrokerMetadataPublisher(config,
        metadataCache,
        logManager,
        replicaManager,
        groupCoordinator,
        transactionCoordinator,
        shareCoordinator,
        sharePartitionManager,
        new DynamicConfigPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          dynamicConfigHandlers.toMap,
        "broker"),
        new DynamicClientQuotaPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          clientQuotaMetadataManager,
        ),
        new DynamicTopicClusterQuotaPublisher(
          clusterId,
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          quotaManagers.clientQuotaCallbackPlugin(),
          quotaManagers.quotaConfigChangeListener()
        ),
        new ScramPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          credentialProvider),
        new DelegationTokenPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          tokenManager),
        new AclPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          authorizerPlugin.toJava
        ),
        sharedServer.initialBrokerMetadataLoadFaultHandler,
        sharedServer.metadataPublishingFaultHandler
      )
      // If the BrokerLifecycleManager's initial catch-up future fails, it means we timed out
      // or are shutting down before we could catch up. Therefore, also fail the firstPublishFuture.
      lifecycleManager.initialCatchUpFuture.whenComplete((_, e) => {
        if (e != null) brokerMetadataPublisher.firstPublishFuture.completeExceptionally(e)
      })
      // 安装 broker metadata publisher。
      metadataPublishers.add(brokerMetadataPublisher)
      // 跟踪 broker registration 变化，必要时触发重新注册。
      brokerRegistrationTracker = new BrokerRegistrationTracker(config.brokerId,
        () => lifecycleManager.resendBrokerRegistration())
      // 安装 broker registration tracker。
      metadataPublishers.add(brokerRegistrationTracker)


      // Register parts of the broker that can be reconfigured via dynamic configs.  This needs to
      // be done before we publish the dynamic configs, so that we don't miss anything.
      config.dynamicConfig.addReconfigurables(this)

      // Install all the metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)

      // Wait for this broker to contact the quorum, and for the active controller to acknowledge
      // us as caught up. It will do this by returning a heartbeat response with isCaughtUp set to
      // true. The BrokerLifecycleManager tracks this.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the controller to acknowledge that we are caught up",
        lifecycleManager.initialCatchUpFuture, startupDeadline, time)

      // Wait for the first metadata update to be published. Metadata updates are not published
      // until we read at least up to the high water mark of the cluster metadata partition.
      // Usually, we publish the initial metadata before lifecycleManager.initialCatchUpFuture
      // is completed, so this check is not necessary. But this is a simple check to make
      // completely sure.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the initial broker metadata update to be published",
        brokerMetadataPublisher.firstPublishFuture , startupDeadline, time)

      // Now that we have loaded some metadata, we can log a reasonably up-to-date broker
      // configuration.  Keep in mind that KafkaConfig.originals is a mutable field that gets set
      // by the dynamic configuration publisher. Ironically, KafkaConfig.originals does not
      // contain the original configuration values.
      new KafkaConfig(config.originals(), true)

      // We're now ready to unfence the broker. This also allows this broker to transition
      // from RECOVERY state to RUNNING state, once the controller unfences the broker.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker to be unfenced",
        lifecycleManager.setReadyToUnfence(), startupDeadline, time)

      // Enable inbound TCP connections. Each endpoint will be started only once its matching
      // authorizer future is completed.
      val endpointReadyFutures = {
        val builder = new EndpointReadyFutures.Builder()
        builder.build(authorizerPlugin.toJava,
          new KafkaAuthorizerServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            listenerInfo.listeners().values(),
            listenerInfo.firstListener(),
            config.earlyStartListeners.map(_.value()).asJava))
      }
      // 提取每个 endpoint 的授权器 ready future。
      val authorizerFutures = endpointReadyFutures.futures().asScala.toMap
      // 在授权器 ready 后开启 SocketServer 请求处理。
      val enableRequestProcessingFuture = socketServer.enableRequestProcessing(authorizerFutures)

      // Block here until all the authorizer futures are complete.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the SocketServer Acceptors to be started",
        enableRequestProcessingFuture, startupDeadline, time)

      // 通知 remote log metadata manager：broker 已经完成启动。
      remoteLogManagerOpt.foreach(rlm =>
        rlm.remoteLogMetadataManager() match {
          case callback: BrokerReadyCallback =>
            try {
              callback.onBrokerReady()
            } catch {
              case e: Exception =>
                error(s"Error executing broker ready callback: ${callback.getClass.getSimpleName}", e)
            }
          case _ => // Skip
        }
      )

      // broker 启动完成，切换为 STARTED。
      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        // 启动失败时先切到 STARTED，便于复用 shutdown 状态机清理已启动组件。
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        // 透传真实异常原因，便于上层看到启动失败根因。
        throw if (e.isInstanceOf[ExecutionException]) e.getCause else e
    }
  }

  private def createPartitionMetadataClient(metadataCache: MetadataCache): PartitionMetadataClient = {
    new NetworkPartitionMetadataClient(
      metadataCache,
      () => NetworkUtils.buildNetworkClient(
        "NetworkPartitionMetadataClient",
        config,
        metrics,
        Time.SYSTEM,
        new LogContext(s"[NetworkPartitionMetadataClient broker=${config.brokerId}]")
      ),
      Time.SYSTEM,
      config.interBrokerListenerName(),
      shareGroupTimer
    )
  }

  private def createGroupCoordinator(): GroupCoordinator = {
    // Create group coordinator, but don't start it until we've started replica manager.
    // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good
    // to fix the underlying issue.
    val time = Time.SYSTEM
    val serde = new GroupCoordinatorRecordSerde
    val timer = new SystemTimerReaper(
      "group-coordinator-reaper",
      new SystemTimer("group-coordinator")
    )
    val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
      time,
      tp => replicaManager.getLog(tp).toJava,
      tp => replicaManager.getLogEndOffset(tp).map(Long.box).toJava,
      serde,
      config.groupCoordinatorConfig.offsetsLoadBufferSize,
      CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
    )
    val writer = new CoordinatorPartitionWriter(
      replicaManager
    )
    new GroupCoordinatorService.Builder(config.brokerId, config.groupCoordinatorConfig)
      .withTime(time)
      .withTimer(timer)
      .withLoader(loader)
      .withWriter(writer)
      .withCoordinatorRuntimeMetrics(new GroupCoordinatorRuntimeMetrics(metrics))
      .withGroupCoordinatorMetrics(new GroupCoordinatorMetrics(KafkaYammerMetrics.defaultRegistry, metrics))
      .withGroupConfigManager(groupConfigManager)
      .withPersister(persister)
      .withAuthorizerPlugin(authorizerPlugin.toJava)
      .withPartitionMetadataClient(partitionMetadataClient)
      .build()
  }

  private def createShareCoordinator(): ShareCoordinator = {
    val time = Time.SYSTEM
    val timer = new SystemTimerReaper(
      "share-coordinator-reaper",
      new SystemTimer("share-coordinator")
    )

    val serde = new ShareCoordinatorRecordSerde
    val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
      time,
      tp => replicaManager.getLog(tp).toJava,
      tp => replicaManager.getLogEndOffset(tp).map(Long.box).toJava,
      serde,
      config.shareCoordinatorConfig.shareCoordinatorLoadBufferSize(),
      CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
    )
    val writer = new CoordinatorPartitionWriter(
      replicaManager
    )
    new ShareCoordinatorService.Builder(config.brokerId, config.shareCoordinatorConfig)
      .withTimer(timer)
      .withTime(time)
      .withLoader(loader)
      .withWriter(writer)
      .withCoordinatorRuntimeMetrics(new ShareCoordinatorRuntimeMetrics(metrics))
      .withCoordinatorMetrics(new ShareCoordinatorMetrics(metrics))
      .build()
  }

  private def createShareStatePersister(): Persister = {
    if (config.shareGroupConfig.shareGroupPersisterClassName.nonEmpty) {
      val klass = Utils.loadClass(config.shareGroupConfig.shareGroupPersisterClassName, classOf[Object]).asInstanceOf[Class[Persister]]
      if (klass.getName.equals(classOf[DefaultStatePersister].getName)) {
        DefaultStatePersister.instance(
          NetworkUtils.buildNetworkClient("Persister", config, metrics, Time.SYSTEM, new LogContext(s"[Persister broker=${config.brokerId}]")),
          new ShareCoordinatorMetadataCacheHelperImpl(metadataCache, key => shareCoordinator.partitionFor(key), config.interBrokerListenerName, groupConfigManager),
          Time.SYSTEM,
          shareGroupTimer
        )
      } else if (klass.getName.equals(classOf[NoOpStatePersister].getName)) {
        info("Using no-op persister")
        new NoOpStatePersister()
      } else {
        error("Unknown persister specified. Persister is only factory-pluggable!")
        throw new IllegalArgumentException("Unknown persister specified " + config.shareGroupConfig.shareGroupPersisterClassName)
      }
    } else {
      // in case share coordinator not enabled or persister class name deliberately empty (key=)
      info("Using no-op persister")
      new NoOpStatePersister()
    }
  }

  private def createShareGroupDLQManager(): ShareGroupDLQManager = {
    if (config.shareGroupConfig.shareGroupDLQManagerClassName.nonEmpty) {
      val klass = Utils.loadClass(config.shareGroupConfig.shareGroupDLQManagerClassName, classOf[Object]).asInstanceOf[Class[ShareGroupDLQManager]]
      if (klass.getName.equals(classOf[DefaultShareGroupDLQManager].getName)) {
        DefaultShareGroupDLQManager.instance(
          NetworkUtils.buildNetworkClient("ShareGroupDLQManager", config, metrics, Time.SYSTEM, new LogContext(s"[ShareGroupDLQManager broker=${config.brokerId}]")),
          new ShareCoordinatorMetadataCacheHelperImpl(metadataCache, key => shareCoordinator.partitionFor(key), config.interBrokerListenerName, groupConfigManager),
          Time.SYSTEM,
          shareGroupTimer,
          shareGroupMetrics,
          shareGroupLogReader
        )
      } else if (klass.getName.equals(classOf[NoOpShareGroupDLQManager].getName)) {
        info("Using no-op share group DLQ manager")
        new NoOpShareGroupDLQManager()
      } else {
        error("Unknown share group DLQ manager specialization specified. ShareGroupDLQManager is only factory-pluggable!")
        throw new IllegalArgumentException("Unknown share group DLQ manager specified " + config.shareGroupConfig.shareGroupDLQManagerClassName)
      }
    } else {
      // in case share group DLQ manager class name deliberately empty (key=)
      info("Using no-op share group DLQ manager")
      new NoOpShareGroupDLQManager()
    }
  }

  protected def createRemoteLogManager(listenerInfo: ListenerInfo): Option[RemoteLogManager] = {
    if (config.remoteLogManagerConfig.isRemoteStorageSystemEnabled) {
      val listenerName = config.remoteLogManagerConfig.remoteLogMetadataManagerListenerName()
      val endpoint = if (listenerName != null) {
        Some(listenerInfo.listeners().values().stream
          .filter(e => ListenerName.normalised(e.listener()).equals(ListenerName.normalised(listenerName)))
          .findFirst()
          .orElseThrow(() => new ConfigException(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
            listenerName, "Should be set as a listener name within valid broker listener name list: " + listenerInfo.listeners().values())))
      } else {
        None
      }

      val rlm = new RemoteLogManager(config.remoteLogManagerConfig, config.brokerId, config.logDirs.get(0), clusterId, time,
        (tp: TopicPartition) => logManager.getLog(tp),
        (tp: TopicPartition, remoteLogStartOffset: java.lang.Long) => {
          logManager.getLog(tp).ifPresent { log =>
            log.updateLogStartOffsetFromRemoteTier(remoteLogStartOffset)
          }
        },
        brokerTopicStats, metrics, endpoint.toJava)
      Some(rlm)
    } else {
      None
    }
  }

  override def shutdown(timeout: Duration): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      val deadline = time.milliseconds() + timeout.toMillis
      info("shutting down")

      if (config.controlledShutdownEnable) {
        if (replicaManager != null)
          replicaManager.beginControlledShutdown()

        if (lifecycleManager != null) {
          lifecycleManager.beginControlledShutdown()
          try {
            val controlledShutdownTimeoutMs = deadline - time.milliseconds()
            lifecycleManager.controlledShutdownFuture.get(controlledShutdownTimeoutMs, TimeUnit.MILLISECONDS)
          } catch {
            case _: TimeoutException =>
              error("Timed out waiting for the controller to approve controlled shutdown")
            case e: Throwable =>
              error("Got unexpected exception waiting for controlled shutdown future", e)
          }
        }
      }
      if (lifecycleManager != null)
        lifecycleManager.beginShutdown()

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        Utils.swallow(this.logger.underlying, () => socketServer.stopProcessingRequests())
      }
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (dataPlaneRequestHandlerPool != null)
        Utils.swallow(this.logger.underlying, () => dataPlaneRequestHandlerPool.shutdown())
      if (dataPlaneRequestProcessor != null)
        Utils.swallow(this.logger.underlying, () => dataPlaneRequestProcessor.close())
      authorizerPlugin.foreach(Utils.closeQuietly(_, "authorizer plugin"))

      /**
       * We must shutdown the scheduler early because otherwise, the scheduler could touch other
       * resources that might have been shutdown and cause exceptions.
       * For example, if we didn't shutdown the scheduler first, when LogManager was closing
       * partitions one by one, the scheduler might concurrently delete old segments due to
       * retention. However, the old segments could have been closed by the LogManager, which would
       * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
       * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
       * broker would have to take hours to recover the log during restart.
       */
      if (kafkaScheduler != null)
        Utils.swallow(this.logger.underlying, () => kafkaScheduler.shutdown())

      if (transactionCoordinator != null)
        Utils.swallow(this.logger.underlying, () => transactionCoordinator.shutdown())

      if (groupConfigManager != null)
        Utils.swallow(this.logger.underlying, () => groupConfigManager.close())

      if (groupCoordinator != null)
        Utils.swallow(this.logger.underlying, () => groupCoordinator.shutdown())

      if (partitionMetadataClient != null)
        Utils.swallow(this.logger.underlying, () => partitionMetadataClient.close())

      if (shareCoordinator != null)
        Utils.swallow(this.logger.underlying, () => shareCoordinator.shutdown())

      if (autoTopicCreationManager != null)
        Utils.swallow(this.logger.underlying, () => autoTopicCreationManager.close())

      if (assignmentsManager != null)
        Utils.swallow(this.logger.underlying, () => assignmentsManager.close())

      if (replicaManager != null)
        Utils.swallow(this.logger.underlying, () => replicaManager.shutdown())

      if (alterPartitionManager != null)
        Utils.swallow(this.logger.underlying, () => alterPartitionManager.shutdown())

      if (forwardingManager != null)
        Utils.swallow(this.logger.underlying, () => forwardingManager.close())

      if (clientToControllerChannelManager != null)
        Utils.swallow(this.logger.underlying, () => clientToControllerChannelManager.shutdown())

      if (logManager != null) {
        val brokerEpoch = if (lifecycleManager != null) lifecycleManager.brokerEpoch else -1
        Utils.swallow(this.logger.underlying, () => logManager.shutdown(brokerEpoch))
      }

      // Close remote log manager to give a chance to any of its underlying clients
      // (especially in RemoteStorageManager and RemoteLogMetadataManager) to close gracefully.
      remoteLogManagerOpt.foreach(Utils.closeQuietly(_, "remote log manager"))

      if (quotaManagers != null)
        Utils.swallow(this.logger.underlying, () => quotaManagers.shutdown())

      if (socketServer != null)
        Utils.swallow(this.logger.underlying, () => socketServer.shutdown())

      Utils.closeQuietly(brokerTopicStats, "broker topic stats")
      Utils.closeQuietly(sharePartitionManager, "share partition manager")

      // The order of closing sharePartitionManager, groupCoordinator and persister matters.
      // groupCoordinator, sharePartitionManager must be closed before the persister so that
      // new requests from sharePartitionManager, groupCoordinator do not encounter a stopped
      // persister.
      if (persister != null)
        Utils.swallow(this.logger.underlying, () => persister.stop())

      // The order of closing sharePartitionManager and shareGroupDLQManager matters.
      // sharePartitionManager must be closed before the shareGroupDLQManager so any new
      // requests from sharePartitionManager do not encounter a stopped shareGroupDLQManager.
      if (shareGroupDLQManager != null)
        Utils.swallow(this.logger.underlying, () => shareGroupDLQManager.stop())

      if (shareGroupMetrics != null)
        Utils.swallow(this.logger.underlying, () => shareGroupMetrics.close())

      Utils.closeQuietly(shareGroupTimer, "share group timer")

      if (lifecycleManager != null)
        Utils.swallow(this.logger.underlying, () => lifecycleManager.close())

      Utils.swallow(this.logger.underlying, () => config.dynamicConfig.clear())
      Utils.closeQuietly(clientMetricsManager, "client metrics manager")
      sharedServer.stopForBroker()
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def isShutdown(): Boolean = {
    status == SHUTDOWN || status == SHUTTING_DOWN
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

}
