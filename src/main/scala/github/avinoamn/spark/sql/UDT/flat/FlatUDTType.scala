package github.avinoamn.spark.sql.UDT.flat

import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.types.StructField
import org.apache.spark.unsafe.types.UTF8String

case class FlatUDTType[T >: Null, V: ScalaReflection.universe.TypeTag](typesCount: Int,
                                                                       index: Int,
                                                                       fromCatalystWrapper: Any => Any,
                                                                       ctor: V => T,
                                                                       dtor: T => Option[V]) {

  private val rowIndex = index + 1
  private val indexString: String = index.toString

  private val dataType = schemaFor[V].dataType
  val structField: StructField = StructField(indexString, dataType, nullable = true)

  private val toCatalyst = CatalystTypeConverters.createToCatalystConverter(dataType)
  private val fromCatalyst = CatalystTypeConverters.createToScalaConverter(dataType)

  private def getValue(value: V): Any  = toCatalyst(value)
  private def getValue(row: InternalRow): Any = fromCatalystWrapper(fromCatalyst(row.get(rowIndex, dataType)))

  def toRow(obj: T): Option[InternalRow] = dtor(obj) match {
    case Some(value) => Some(InternalRow(
      UTF8String.fromString(indexString) +:
      Seq.fill(typesCount)(null).updated(index, getValue(value)): _*
    ))
    case None => None
  }

  def fromRow(row: InternalRow): T = ctor(getValue(row).asInstanceOf[V])

}
