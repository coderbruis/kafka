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

import kafka.network.RequestChannel
import kafka.utils.Logging
import kafka.server.KafkaRequestHandler.{threadCurrentRequest, threadRequestChannel}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.internals.{Exit, KafkaThread}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.network.{CallbackRequest, Request, ShutdownRequest, WakeupRequest}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.OptionalLong
import scala.collection.mutable

trait ApiRequestHandler {
  def handle(request: Request, requestLocal: RequestLocal): Unit
  def tryCompleteActions(): Unit = {}
}

object KafkaRequestHandler {
  // Support for scheduling callbacks on a request thread.
  private val threadRequestChannel = new ThreadLocal[RequestChannel]
  private val threadCurrentRequest = new ThreadLocal[Request]

  // For testing
  @volatile private var bypassThreadCheck = false
  def setBypassThreadCheck(bypassCheck: Boolean): Unit = {
    bypassThreadCheck = bypassCheck
  }

  /**
   * Creates a wrapped callback to be executed synchronously on the calling request thread or asynchronously
   * on an arbitrary request thread.
   * NOTE: this function must be originally called from a request thread.
   * @param asyncCompletionCallback A callback method that we intend to call from the current thread or in another
   *                                thread after an asynchronous action completes. The RequestLocal passed in must
   *                                belong to the request handler thread that is executing the callback.
   * @param requestLocal The RequestLocal for the current request handler thread in case we need to execute the callback
   *                     function synchronously from the calling thread.
   * @return Wrapped callback will either immediately execute `asyncCompletionCallback` or schedule it on an arbitrary request thread
   *         depending on where it is called
   */
  def wrapAsyncCallback[T](asyncCompletionCallback: (RequestLocal, T) => Unit, requestLocal: RequestLocal): T => Unit = {
    val requestChannel = threadRequestChannel.get()
    val currentRequest = threadCurrentRequest.get()
    if (requestChannel == null || currentRequest == null) {
      if (!bypassThreadCheck)
        throw new IllegalStateException("Attempted to reschedule to request handler thread from non-request handler thread.")
      t => asyncCompletionCallback(requestLocal, t)
    } else {
      t => {
        if (threadCurrentRequest.get() == currentRequest) {
          // If the callback is actually executed on the same request thread, we can directly execute
          // it without re-scheduling it.
          asyncCompletionCallback(requestLocal, t)
        } else {
          // The requestChannel and request are captured in this lambda, so when it's executed on the callback thread
          // we can re-schedule the original callback on a request thread and update the metrics accordingly.
          requestChannel.sendCallbackRequest(new CallbackRequest(newRequestLocal => asyncCompletionCallback(newRequestLocal, t), currentRequest))
        }
      }
    }
  }
}

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(
  id: Int,
  brokerId: Int,
  val aggregateIdleMeter: Meter,
  val aggregateThreads: AtomicInteger,
  val poolIdleMeter: Meter,
  val poolHandlerThreads: AtomicInteger,
  val requestChannel: RequestChannel,
  apis: ApiRequestHandler,
  time: Time,
  nodeName: String
) extends Runnable with Logging {
  this.logIdent = s"[Kafka Request Handler $id on ${nodeName.capitalize} $brokerId] "
  private val shutdownComplete = new CountDownLatch(1)
  private val requestLocal = RequestLocal.withThreadConfinedCaching
  @volatile private var stopped = false

  def run(): Unit = {
    // 绑定当前请求线程使用的 RequestChannel，供异步 callback 回到请求线程执行。
    threadRequestChannel.set(requestChannel)
    // 请求处理线程主循环，持续从 RequestChannel 拉取请求。
    while (!stopped) {
      // 记录等待请求前的时间，用于计算请求处理线程空闲率。
      val startSelectTime = time.nanoseconds

      // 从 RequestChannel 拉取下一个请求，最多等待 300ms。
      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      // 计算本次等待请求的空闲时间。
      val idleTime = endTime - startSelectTime
      // 记录当前 handler pool 的空闲率。
      poolIdleMeter.mark(idleTime / poolHandlerThreads.get)
      // 记录所有 request handler pool 聚合后的空闲率。
      aggregateIdleMeter.mark(idleTime / aggregateThreads.get)

      req match {
        case _: ShutdownRequest =>
          // 收到关闭请求时完成线程清理并退出循环。
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          completeShutdown()
          return

        case callback: CallbackRequest =>
          // 处理异步操作完成后重新调度到请求线程的 callback。
          val originalRequest = callback.originalRequest
          try {

            // 更新 callback 出队时间，支持同一请求多次 callback 的耗时统计。
            if (originalRequest.callbackRequestDequeueTimeNanos.isPresent) {
              val prevCallbacksTimeNanos = originalRequest.callbackRequestCompleteTimeNanos.orElse(0L) -
                originalRequest.callbackRequestDequeueTimeNanos.orElse(0L)
              originalRequest.callbackRequestCompleteTimeNanos(OptionalLong.empty)
              originalRequest.callbackRequestDequeueTimeNanos(OptionalLong.of(time.nanoseconds() - prevCallbacksTimeNanos))
            } else {
              originalRequest.callbackRequestDequeueTimeNanos(OptionalLong.of(time.nanoseconds()))
            }

            // 标记当前线程正在处理的原始请求，供 callback 逻辑获取上下文。
            threadCurrentRequest.set(originalRequest)
            // 执行被重新调度到请求线程的 callback。
            callback.fun().accept(requestLocal)
          } catch {
            case e: FatalExitError =>
              // 致命退出异常直接完成清理并退出进程。
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            // callback 执行后尝试推进延迟动作。
            apis.tryCompleteActions()
            // 记录 callback 完成时间，避免响应指标缺失。
            if (originalRequest.callbackRequestCompleteTimeNanos.isEmpty)
              originalRequest.callbackRequestCompleteTimeNanos(OptionalLong.of(time.nanoseconds()))
            // 清理当前线程请求上下文。
            threadCurrentRequest.remove()
          }

        case request: Request =>
          // 处理普通 Kafka 请求，进入 KafkaApis 业务分发。
          try {
            // 记录请求从 RequestChannel 出队时间。
            request.requestDequeueTimeNanos(endTime)
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            // 标记当前线程正在处理的请求。
            threadCurrentRequest.set(request)
            // 调用 KafkaApis，根据 ApiKey 分发到对应业务逻辑。
            apis.handle(request, requestLocal)
          } catch {
            case e: FatalExitError =>
              // 致命退出异常直接完成清理并退出进程。
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            // 清理当前线程请求上下文。
            threadCurrentRequest.remove()
            // 释放请求持有的网络 buffer。
            request.releaseBuffer()
          }

        case _: WakeupRequest =>
          // WakeupRequest 通常应在 receiveRequest 内部消费，这里只记录异常路径。
          warn("Received a wakeup request outside of typical usage.")

        case null => // 本轮没有请求，继续下一次轮询。
      }
    }
    // stopped 后完成请求线程清理。
    completeShutdown()
  }

  private def completeShutdown(): Unit = {
    requestLocal.close()
    threadRequestChannel.remove()
    shutdownComplete.countDown()
  }

  def stop(): Unit = {
    stopped = true
  }

  def initiateShutdown(): Unit = requestChannel.sendShutdownRequest()

  def awaitShutdown(): Unit = shutdownComplete.await()

}

/**
 * Factory for creating KafkaRequestHandlerPool instances with shared aggregate metrics.
 * All pools created by the same factory share the same aggregateThreads counter.
 */
class KafkaRequestHandlerPoolFactory {
  private[this] val aggregateThreads = new AtomicInteger(0)
  private[this] val RequestHandlerAvgIdleMetricName = "RequestHandlerAvgIdlePercent"
  
  def createPool(
    brokerId: Int,
    requestChannel: RequestChannel,
    apis: ApiRequestHandler,
    time: Time,
    numThreads: Int,
    nodeName: String
  ): KafkaRequestHandlerPool = {
    new KafkaRequestHandlerPool(aggregateThreads, RequestHandlerAvgIdleMetricName, brokerId, requestChannel, apis, time, numThreads, nodeName)
  }

  // Only used for test purpose
  def aggregateThreadCount: Int = aggregateThreads.get()
}

class KafkaRequestHandlerPool(
  val aggregateThreads: AtomicInteger,
  val requestHandlerAvgIdleMetricName: String,
  val brokerId: Int,
  val requestChannel: RequestChannel,
  val apis: ApiRequestHandler,
  time: Time,
  numThreads: Int,
  nodeName: String
) extends Logging {
  // Changing the package or class name may cause incompatibility with existing code and metrics configuration
  private val metricsPackage = "kafka.server"
  private val metricsClassName = "KafkaRequestHandlerPool"
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)

  val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  private val perPoolIdleMeterName = if (nodeName == "broker") {
    "BrokerRequestHandlerAvgIdlePercent"
  } else if (nodeName == "controller") {
    "ControllerRequestHandlerAvgIdlePercent"
  } else {
    throw new IllegalArgumentException("Invalid node name:" + nodeName)
  }
  /* Per-pool idle meter (broker-only or controller-only) */
  private val perPoolIdleMeter = metricsGroup.newMeter(perPoolIdleMeterName, "percent", TimeUnit.NANOSECONDS)
  /* Aggregate meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = metricsGroup.newMeter(requestHandlerAvgIdleMetricName, "percent", TimeUnit.NANOSECONDS)

  this.logIdent = s"[data-plane Kafka Request Handler on ${nodeName.capitalize} $brokerId] "
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    createHandler(i)
  }

  private def createHandler(id: Int): Unit = {
    runnables += new KafkaRequestHandler(
      id,
      brokerId,
      aggregateIdleMeter,
      aggregateThreads,
      perPoolIdleMeter,
      threadPoolSize,
      requestChannel,
      apis,
      time,
      nodeName
    )
    aggregateThreads.getAndIncrement()
    KafkaThread.daemon("data-plane-kafka-request-handler-" + id, runnables(id)).start()
  }

  private def deleteHandler(id: Int): Unit = {
    runnables.remove(id).stop()
    aggregateThreads.getAndDecrement()
  }

  def resizeThreadPool(newSize: Int): Unit = synchronized {
    val currentSize = threadPoolSize.get
    info(s"Resizing request handler thread pool size from $currentSize to $newSize")
    if (newSize > currentSize) {
      for (i <- currentSize until newSize) {
        createHandler(i)
      }
    } else if (newSize < currentSize) {
      for (i <- 1 to (currentSize - newSize)) {
        deleteHandler(currentSize - i)
      }
    }
    threadPoolSize.set(newSize)
  }

  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    // Unregister this pool's threads from shared aggregate counter
    aggregateThreads.addAndGet(-threadPoolSize.get)
    info("shut down completely")
  }
}
