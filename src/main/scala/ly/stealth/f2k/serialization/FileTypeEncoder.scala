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

import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.generic.GenericData.Record
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory
import org.apache.avro.Schema.Parser
import ly.stealth.f2k.FileTypes
import com.google.protobuf.ByteString
import java.nio.ByteBuffer

object FileTypeEncoder {
  def encoder(encoderType: String): FileTypeEncoder = encoderType match {
    case "avro" => AvroTypeFileTypeEncoder
    case "protobuf" => ProtobufFileTypeEncoder
    case _ => throw new IllegalArgumentException("%s encoder type is not supported. Use avro or protobuf instead".format(encoderType))
  }
}

trait FileTypeEncoder {
  def encode(record: FileAndMetadata): Array[Byte]
}

object AvroTypeFileTypeEncoder extends FileTypeEncoder {
  private lazy val schema = new Parser().parse(Thread.currentThread.getContextClassLoader.getResourceAsStream("avro/file.asvc"))
  private lazy val writer = new GenericDatumWriter[Record](schema)

  override def encode(record: FileAndMetadata): Array[Byte] = {
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("data", ByteBuffer.wrap(record.data))

    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(avroRecord, encoder)
    encoder.flush()
    out.flush()
    out.toByteArray
  }
}

object ProtobufFileTypeEncoder extends FileTypeEncoder {
  override def encode(record: FileAndMetadata): Array[Byte] = {
    val builder = FileTypes.FileType.newBuilder()
    val protoRecord = builder.setData(ByteString.copyFrom(record.data)).build()

    protoRecord.toByteArray
  }
}
