/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.f2k

import kafka.utils.Logging
import java.util.Properties
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.Schema.Parser
import java.io._
import java.nio.file.{Path, StandardOpenOption, Paths, Files}
import org.apache.avro.io.DecoderFactory
import kafka.consumer.Whitelist
import java.util.concurrent.TimeUnit
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.generic.GenericData.Record

class KafkaDownloader(topic: String,
                      groupId: String,
                      zookeeperConnect: String,
                      zkSessionTimeoutMs: Int = 30000,
                      readFromStartOfStream: Boolean = true) extends Logging {
  val props = new Properties()
  props.put("group.id", groupId)
  props.put("zookeeper.connect", zookeeperConnect)
  props.put("auto.offset.reset", if (readFromStartOfStream) "smallest" else "largest")
  props.put("zookeeper.session.timeout.ms", zkSessionTimeoutMs.toString)

  val config = new ConsumerConfig(props)
  val connector = Consumer.create(config)

  val filterSpec = new Whitelist(topic)
  val maxWaitTimeout = 15000

  var lastUpdate = 0L

  info("Trying to start consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new StringDecoder(), new DefaultDecoder()).head
  info("Started consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))

  val schema = new Parser().parse(Thread.currentThread.getContextClassLoader.getResourceAsStream("avro/file.asvc"))
  val datumReader = new GenericDatumReader[Record](schema)
  val datum = new GenericData.Record(schema)

  def download(pathToDownload: Path) = {
    val it = stream.iterator()
    try {
      var current = Paths.get("")
      var out: BufferedOutputStream = null

      lastUpdate = System.currentTimeMillis()
      val watcher = new Thread(new ConsumerWatcher)
      watcher.setDaemon(true)
      watcher.start()
      while (it.hasNext()) {
        debug("Trying to download file bit")
        val messageAndTopic = it.next()
        debug("Downloaded file bit")
        val path = Paths.get(messageAndTopic.key())
        if (path.startsWith(pathToDownload)) {
          if (!messageAndTopic.message().isEmpty) {
            debug("Trying to decode file bit")
            val decoder = DecoderFactory.get().binaryDecoder(messageAndTopic.message(), null)
            val record = datumReader.read(datum, decoder)
            debug("File bit has been decoded")

            val parent = path.getParent
            if (!Files.exists(parent)) {
              trace("Directory %s does not exist, trying to create".format(parent.toString))
              Files.createDirectories(parent)
              trace("Сreated directory %s".format(parent.toString))
            }

            if (path != current) {
              debug("File %s has been successfully downloaded".format(current.toString))

              trace("Trying to close out for file %s".format(current.toString))
              if (out != null) out.close()
              trace("Сlosed out for file %s".format(current.toString))

              current = path

              trace("Trying to create new out for file %s".format(current.toString))
              out = new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE))
              trace("Created new out for file %s".format(current.toString))
            }

            debug("Trying to write data for file %s".format(path.toString))
            out.write(record.get("data").asInstanceOf[ByteBuffer].array())
            out.flush()
            debug("Wrote data for file %s".format(path.toString))

            lastUpdate = System.currentTimeMillis()
          }
        }
      }
    } catch {
      case e: Exception => {
        warn("Consumer has been stopped", e)
      }
    }

    info("Files has been successfully downloaded")
  }

  def close() = {
    info("Shutting down consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))
    connector.shutdown()
    info("Shut down consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))
  }

  class ConsumerWatcher extends Runnable {
    override def run() {
      while (!Thread.currentThread().isInterrupted) {
        if (System.currentTimeMillis() - lastUpdate > maxWaitTimeout) {
          close()
          return
        } else {
          TimeUnit.MILLISECONDS.sleep(maxWaitTimeout)
        }
      }
    }
  }
}
