package org.sireum.hamr.inspector.capabilities.redis

import java.util.concurrent.Executors

import art.Art.PortId
import art.{Art, ArtDebug, DataContent}
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.pubsub.{RedisPubSubAdapter, StatefulRedisPubSubConnection}
import org.sireum.Z
import org.sireum.hamr.inspector.capabilities.{InspectorHAMRLauncher, ProjectListener}

final class DirectProjectListener extends ProjectListener {

  private val redisClient = RedisClient.create("redis://localhost:6379/0")
  private val connection: StatefulRedisConnection[String, String] = redisClient.connect()
  private val pubSubConnection: StatefulRedisPubSubConnection[String, String] = redisClient.connectPubSub()
  private val commands: RedisCommands[String, String] = connection.sync()

  Commands.init(commands)

  private val executorThreadGroup = new ThreadGroup("Inspector")
  private val executorThreadName = "inspector-capabilities-redis-EXECUTOR"

  private val executor =
    Executors.newSingleThreadExecutor((r: Runnable) => new Thread(executorThreadGroup, r, executorThreadName))

  override def start(time: Art.Time): Unit = {
    Commands.start(time.toString)

    pubSubConnection.addListener(new RedisPubSubAdapter[String, String] {
      override def message(channel: String, message: String): Unit = {

        println(s"Inspector-Capabilities-Redis INFO: handling remote injection: $message")

        val split = message.split(',')

        if (split.length < 3) {
          println("Inspector-Capabilities-Redis WARNING: DirectProjectListener received malformed message {}", message)
          return
        }

        val bridgeId: Art.BridgeId = Z.apply(split(0)).getOrElse(Z(-1))
        val portId: Art.PortId = Z.apply(split(1)).getOrElse(Z(-1))

        val dataContentStartIndex = split(0).length + split(1).length + 2 // plus 2 for extra two commas in init String
        val dataContentString = message.substring(dataContentStartIndex)
        val dataContent = InspectorHAMRLauncher.deserializer(dataContentString)

        ArtDebug.injectPort(bridgeId, portId, dataContent)
      }
    })

    pubSubConnection.sync().subscribe(Commands.incomingChannelKey)
  }

  override def stop(time: Art.Time): Unit = {
    Commands.stop(time.toString, "Natural Termination")
  }

  override def output(src: PortId, dst: PortId, data: DataContent, time: Art.Time): Unit = {
    executor.execute(() => {
      Commands.xadd(time.toString, src.toLong, dst.toLong, data)
    })
  }

}