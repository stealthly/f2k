package ly.stealth.f2k


object Main extends App {
  if (args.length < 4)
    throw new IllegalArgumentException("Usage: (download|upload) (pathToUpload|directoryToDownload) kafkaTopic (kafkaConnect|zookeeperConnect) (downloadLocation)")

  if (args(0) == "upload") {
    val uploader = new KafkaUploader(args(3))
    uploader.upload(args(1), args(2))
  } else if (args(0) == "download") {
    val downloader = new KafkaDownloader(args(2), "group", args(3))
    downloader.download(args(1), args(4))
    downloader.close()
  } else {
    throw new IllegalArgumentException("You should provide either upload or download option")
  }
}
