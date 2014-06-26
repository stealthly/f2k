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
import java.util.jar.{JarEntry, JarOutputStream}
import kafka.consumer.Whitelist
import java.util.concurrent.TimeUnit

class KafkaJarDownloader(topic: String,
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

  val tmpPath = Paths.get("/tmp")
  
  val schema = new Parser().parse(Thread.currentThread.getContextClassLoader.getResourceAsStream("avro/file.asvc"))
  val datumReader = new GenericDatumReader[GenericRecord](schema)
  val datum = new GenericData.Record(schema)

  def download(jarName: String) = {
    val basePath = tmpPath.resolve(jarName.replace(".jar", ""))
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
        if (messageAndTopic.key() == jarName) {
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

    new JarCreator(jarName, basePath.toString).create()
  }

  def close() = {
    info("Shutting down consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))
    connector.shutdown()
    info("Shut down consumer: topic=%s for zk=%s and groupId=%s".format(topic, zookeeperConnect, groupId))
  }

  class JarCreator(jarName: String, srcDir: String) {

    def create() = {
      info("Trying to create jar")
      val manifest: jar.Manifest = createManifest(srcDir)
      val target = new JarOutputStream(new FileOutputStream(jarName), manifest)
      add(new File(srcDir), target)
      target.close()
      info("Jar has been created")
    }

    private def createManifest(srcDir: String): java.util.jar.Manifest = {
      new java.util.jar.Manifest(new FileInputStream(Paths.get(srcDir).resolve("META-INF/MANIFEST.MF").toFile))
    }

    private def add(source: File, target: JarOutputStream) {
      var in: BufferedInputStream = null
      try {
        if (source.isDirectory) {
          var name: String = source.getPath.replace(srcDir, "").replace("\\", "/")
          if (!name.isEmpty) {
            if (!name.endsWith("/")) name += "/"
            val entry = new JarEntry(name)
            entry.setTime(source.lastModified)
            target.putNextEntry(entry)
            target.closeEntry()
          }
          for (nestedFile <- source.listFiles) add(nestedFile, target)

          return
        }

        if (source.getName == "MANIFEST.MF") return

        val entry = new JarEntry(source.getPath.replace(srcDir, "").replace("\\", "/"))
        entry.setTime(source.lastModified)
        target.putNextEntry(entry)
        in = new BufferedInputStream(new FileInputStream(source))
        val buffer: Array[Byte] = new Array[Byte](1024)
        while (true) {
          val count: Int = in.read(buffer)
          if (count == -1) return
          target.write(buffer, 0, count)
        }
        target.closeEntry()
      } finally {
        if (in != null) in.close()
      }
    }
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
