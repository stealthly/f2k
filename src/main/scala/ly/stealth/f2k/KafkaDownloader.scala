package ly.stealth.f2k

import kafka.utils.Logging
import java.util.{jar, Properties}
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.serializer.StringDecoder
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.Schema.Parser
import java.io._
import java.nio.file.{StandardOpenOption, Paths, Files}
import org.apache.avro.io.DecoderFactory
import java.util.jar.JarEntry
import kafka.consumer.Whitelist
import java.util.concurrent.TimeUnit

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

  def download(pathToDownload: String, destination: String) = {
    val basePath = Paths.get(destination).resolve(pathToDownload)
    if (!Files.exists(basePath)) {
      Files.createDirectory(basePath)
    }
    val it = stream.iterator()
    try {
      var currentFile = ""
      var writer: BufferedWriter = null

      lastUpdate = System.currentTimeMillis()
      val watcher = new Thread(new ConsumerWatcher)
      watcher.setDaemon(true)
      watcher.start()
      while (it.hasNext()) {
        debug("Trying to download file bit")
        val messageAndTopic = it.next()
        debug("Downloaded file bit")
        if (messageAndTopic.key() == pathToDownload) {
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

              trace("Trying to close writer for file %s".format(currentFile))
              if (writer != null) writer.close()
              trace("Сlosed writer for file %s".format(currentFile))

              currentFile = record.get("name").toString

              trace("Trying to create new writer for file %s".format(currentFile))
              writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(path.resolve(record.get("name").toString),
                                                                                       StandardOpenOption.APPEND, StandardOpenOption.CREATE)))
              trace("Created new writer for file %s".format(currentFile))
            }

            debug("Trying to write data for file %s".format(currentFile))
            writer.write(record.get("data").toString)
            writer.newLine()
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
          TimeUnit.SECONDS.sleep(5)
        }
      }
    }
  }
}
