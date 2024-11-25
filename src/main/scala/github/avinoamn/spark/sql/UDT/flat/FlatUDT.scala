package github.avinoamn.spark.sql.UDT.flat

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StringType, StructType, UserDefinedType}

import scala.reflect.classTag

abstract class FlatUDT[T >: Null](implicit m: Manifest[T]) extends UserDefinedType[T] {
  private def udtName: String = getClass.getSimpleName.dropRight(1)

  def types: Array[FlatUDTType[T, _]]

  override def sqlType: DataType = {
    types.foldLeft(new StructType().add("typeIndex", StringType))((acc, tpe) => acc.add(tpe.structField))
  }

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def serialize(obj: T): InternalRow = {
    if (obj == null) return  null

    val rows = for {tpe <- types; row = tpe.toRow(obj); if row.isDefined} yield row.get
    require(rows.length <= 1, s"$udtName cannot have more than 1 udt types for an obj to serialize")

    if (rows.isEmpty) null else rows.head
  }

  override def deserialize(datum: Any): T = {
    if (datum == null) return null

    val row = datum.asInstanceOf[InternalRow]
    val typeIndex = row.getUTF8String(0).toString.toInt
    require(typeIndex < types.length, s"$udtName type with index '$typeIndex' does not exist")

    types(typeIndex).fromRow(row)
  }
}
