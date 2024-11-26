package github.avinoamn.spark.sql.UDT.flat

import org.apache.spark.sql.catalyst.ScalaReflection

trait FlatUDTTypeFactory[T >: Null] {
  def typesCount: Int

  def fromCatalystWrapper(fromCatalyst: Any): Any = fromCatalyst

  def defaultCtor[V]: V => T = null
  def defaultDtor[V]: T => Option[V] = null

  private final var index = -1
  final def createType[V: ScalaReflection.universe.TypeTag]
                      (ctor: V => T = defaultCtor[V],
                       dtor: T => Option[V] = defaultDtor[V]): FlatUDTType[T, V] = {

    index += 1
    require(index < typesCount, s"You're trying to create more types than defined `typesCount` ($typesCount)")

    FlatUDTType[T, V](
      typesCount = typesCount,
      index = index,
      fromCatalystWrapper = fromCatalystWrapper,
      ctor = Option(ctor).getOrElse(throw new Exception(s"No ctor defined for udt type with index $index")),
      dtor = Option(dtor).getOrElse(throw new Exception(s"No dtor defined for udt type with index $index"))
    )
  }
}
