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
import kafka.serializer.StringDecoder
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.Schema.Parser
import java.io._
import java.nio.file.{StandardOpenOption, Paths, Files}
import org.apache.avro.io.DecoderFactory
import kafka.consumer.Whitelist
import java.util.concurrent.TimeUnit
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

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
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new StringDecoder(), new StringDecoder()).head
  info("Started consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))

  val schema = new Parser().parse(Thread.currentThread.getContextClassLoader.getResourceAsStream("avro/file.asvc"))
  val datumReader = new GenericDatumReader[GenericRecord](schema)
  val datum = new GenericData.Record(schema)

  def download(pathToDownload: String, destination: String, partition: Int) = {
    val basePath = Paths.get(destination).resolve(pathToDownload)
    if (!Files.exists(basePath)) {
      Files.createDirectory(basePath)
    }
    val it = stream.iterator()
    try {
      var currentFile = ""
      var out: BufferedOutputStream = null

      lastUpdate = System.currentTimeMillis()
      val watcher = new Thread(new ConsumerWatcher)
      watcher.setDaemon(true)
      watcher.start()
      while (it.hasNext()) {
        debug("Trying to download file bit")
        val messageAndTopic = it.next()
        debug("Downloaded file bit")
        if (messageAndTopic.key() == pathToDownload && messageAndTopic.partition == partition) {
          if (!messageAndTopic.message().isEmpty) {
            debug("Trying to decode file bit")
            val decoder = DecoderFactory.get().jsonDecoder(schema, messageAndTopic.message())
            val record = datumReader.read(datum, decoder)
            debug("File bit has been decoded")

            val relativePath = record.get("path").toString
            val path = if (relativePath == "/") basePath else basePath.resolve(record.get("path").toString)
            if (!Files.exists(path)) {
              trace("Directory %s does not exist, trying to create".format(path.toString))
              Files.createDirectories(path)
              trace("Сreated directory %s".format(path.toString))
            }

            if (currentFile != record.get("name").toString) {
              debug("File %s has been successfully downloaded".format(currentFile))

              trace("Trying to close out for file %s".format(currentFile))
              if (out != null) out.close()
              trace("Сlosed out for file %s".format(currentFile))

              currentFile = record.get("name").toString

              trace("Trying to create new out for file %s".format(currentFile))
              out = new BufferedOutputStream(Files.newOutputStream(path.resolve(record.get("name").toString),
                                                                      StandardOpenOption.APPEND, StandardOpenOption.CREATE))
              trace("Created new out for file %s".format(currentFile))
            }

            debug("Trying to write data for file %s".format(currentFile))
            out.write(record.get("data").asInstanceOf[ByteBuffer].array())
            out.flush()
            debug("Wrote data for file %s".format(currentFile))

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
