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

package ly.stealth.f2k.serialization

import org.apache.avro.generic.{GenericDatumReader, GenericData}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema.Parser
import ly.stealth.f2k.FileTypes
import java.nio.ByteBuffer
import ly.stealth.f2k.FileTypes.FileType

object FileTypeDecoder {
  def decoder(decoderType: String): FileTypeDecoder = decoderType match {
    case "avro" => AvroFileTypeDecoder
    case "protobuf" => ProtobufFileTypeDecoder
    case _ => throw new IllegalArgumentException("%s decoder type is not supported. Use avro or protobuf instead".format(decoderType))
  }
}

trait FileTypeDecoder {
  def decode(in: Array[Byte]): FileAndMetadata
}

object AvroFileTypeDecoder extends FileTypeDecoder {
  private lazy val schemaIn = Thread.currentThread.getContextClassLoader.getResourceAsStream("avro/file.asvc")
  private lazy val schema = new Parser().parse(schemaIn)
  private lazy val datumReader = new GenericDatumReader[Record](schema)

  override def decode(in: Array[Byte]): FileAndMetadata = {
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    val record: Record = datumReader.read(null, decoder)
    new FileAndMetadata(record.get("data").asInstanceOf[ByteBuffer].array())
  }
}

object ProtobufFileTypeDecoder extends FileTypeDecoder {
  override def decode(in: Array[Byte]): FileAndMetadata = {
    val record: FileType = FileTypes.FileType.parseFrom(in)
    new FileAndMetadata(record.getData.toByteArray)
  }
}
