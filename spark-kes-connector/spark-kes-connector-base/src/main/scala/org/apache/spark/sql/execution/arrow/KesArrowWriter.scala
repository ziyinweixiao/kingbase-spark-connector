/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.arrow

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.KesArrowUtils

import scala.collection.JavaConverters._

/**
 * Copied from Spark 3.1.2. To avoid the package conflicts between spark 2 and spark 3.
 */
object KesArrowWriter {

  def create(schema: StructType, timeZoneId: String): KesArrowWriter = {
    val arrowSchema = KesArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, KesArrowUtils.rootAllocator)
    create(root)
  }

  def create(root: VectorSchemaRoot): KesArrowWriter = {
    val children = root.getFieldVectors().asScala.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }
    new KesArrowWriter(root, children.toArray)
  }

  private def createFieldWriter(vector: ValueVector): KesArrowFieldWriter = {
    val field = vector.getField()
    (KesArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector) => new KesBooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new KesByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new KesShortWriter(vector)
      case (IntegerType, vector: IntVector) => new KesIntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new KesLongWriter(vector)
      case (FloatType, vector: Float4Vector) => new KesFloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new KesDoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new KesDecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector) => new KesStringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new KesBinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new KesDateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new KesTimestampWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new KesArrayWriter(vector, elementVector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
        val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
        new KesMapWriter(vector, structVector, keyWriter, valueWriter)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new KesStructWriter(vector, children.toArray)
      case (dt, _) =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
    }
  }
}

class KesArrowWriter(val root: VectorSchemaRoot, fields: Array[KesArrowFieldWriter]) {

  def schema: StructType = StructType(fields.map { f =>
    StructField(f.name, f.dataType, f.nullable)
  })

  private var count: Int = 0

  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.size) {
      fields(i).write(row, i)
      i += 1
    }
    count += 1
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }
}

private[spark] abstract class KesArrowFieldWriter {

  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def dataType: DataType = KesArrowUtils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit

  private[spark] var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }
}

private[spark] class KesBooleanWriter(val valueVector: BitVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[spark] class KesByteWriter(val valueVector: TinyIntVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }
}

private[spark] class KesShortWriter(val valueVector: SmallIntVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }
}

private[spark] class KesIntegerWriter(val valueVector: IntVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[spark] class KesLongWriter(val valueVector: BigIntVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[spark] class KesFloatWriter(val valueVector: Float4Vector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }
}

private[spark] class KesDoubleWriter(val valueVector: Float8Vector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }
}

private[spark] class KesDecimalWriter(
                                    val valueVector: DecimalVector,
                                    precision: Int,
                                    scale: Int) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val decimal = input.getDecimal(ordinal, precision, scale)
    if (decimal.changePrecision(precision, scale)) {
      valueVector.setSafe(count, decimal.toJavaBigDecimal)
    } else {
      setNull()
    }
  }
}

private[spark] class KesStringWriter(val valueVector: VarCharVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[spark] class KesBinaryWriter(
                                   val valueVector: VarBinaryVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[spark] class KesDateWriter(val valueVector: DateDayVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[spark] class KesTimestampWriter(
                                      val valueVector: TimeStampMicroTZVector) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[spark] class KesArrayWriter(
                                  val valueVector: ListVector,
                                  val elementWriter: KesArrowFieldWriter) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[spark] class KesStructWriter(
                                   val valueVector: StructVector,
                                   children: Array[KesArrowFieldWriter]) extends KesArrowFieldWriter {

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    valueVector.setIndexDefined(count)
    while (i < struct.numFields) {
      children(i).write(struct, i)
      i += 1
    }
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}

private[spark] class KesMapWriter(
                                val valueVector: MapVector,
                                val structVector: StructVector,
                                val keyWriter: KesArrowFieldWriter,
                                val valueWriter: KesArrowFieldWriter) extends KesArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val map = input.getMap(ordinal)
    valueVector.startNewValue(count)
    val keys = map.keyArray()
    val values = map.valueArray()
    var i = 0
    while (i <  map.numElements()) {
      structVector.setIndexDefined(keyWriter.count)
      keyWriter.write(keys, i)
      valueWriter.write(values, i)
      i += 1
    }

    valueVector.endValue(count, map.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    keyWriter.finish()
    valueWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    keyWriter.reset()
    valueWriter.reset()
  }
}
