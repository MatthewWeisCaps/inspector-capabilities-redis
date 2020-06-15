/*
 * Copyright (c) 2020, Matthew Weis, Kansas State University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sireum.hamr.inspector.capabilities.redis

import java.util.concurrent.Executors

import art.Art.PortId
import art.{Art, ArtDebug, DataContent}
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.pubsub.{RedisPubSubAdapter, StatefulRedisPubSubConnection}
import org.sireum.Z
import org.sireum.hamr.inspector.capabilities.{InspectorCapabilitiesLauncher, ProjectListener}

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
        val dataContent = InspectorCapabilitiesLauncher.deserializer(dataContentString)

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