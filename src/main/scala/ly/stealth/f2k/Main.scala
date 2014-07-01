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

object Main extends App {
  if (args.length < 5)
    throw new IllegalArgumentException("Usage: (download|upload) (pathToUpload|directoryToDownload) kafkaTopic (kafkaConnect|zookeeperConnect) (downloadLocation|metadataOnly)")

  if (args(0) == "upload") {
    val uploader = new KafkaUploader(args(3))
    uploader.upload(args(1), args(2), args(4).toBoolean)
  } else if (args(0) == "download") {
    val downloader = new KafkaDownloader(args(2), "group", args(3))
    downloader.download(args(1), args(4))
    downloader.close()
  } else {
    throw new IllegalArgumentException("You should provide either upload or download option")
  }
}
