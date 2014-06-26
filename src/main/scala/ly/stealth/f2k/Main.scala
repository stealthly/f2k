package ly.stealth.f2k


object Main extends App {
  if (args.length < 4)
    throw new IllegalArgumentException("Usage: (download|upload) (pathToUploadJar|jarNameToDownload) kafkaTopic (kafkaConnect|zookeeperConnect)")

  if (args(0) == "upload") {
    val uploader = new KafkaJarUploader(args(3))
    uploader.upload(args(1), args(2))
  } else if (args(0) == "download") {
    val downloader = new KafkaJarDownloader(args(2), "group", args(3))
    downloader.download(args(1))
    downloader.close()
  } else {
    throw new IllegalArgumentException("You should provide either upload or download option")
  }
}
