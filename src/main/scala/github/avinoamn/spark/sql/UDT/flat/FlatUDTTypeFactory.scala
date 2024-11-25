package github.avinoamn.spark.sql.UDT.flat

import org.apache.spark.sql.catalyst.ScalaReflection

trait FlatUDTTypeFactory[T >: Null] {
  def typesCount: Int

  def fromCatalystWrapper(fromCatalyst: Any): Any = fromCatalyst

  def defaultCtor[V: ScalaReflection.universe.TypeTag]: Option[V => T] = None
  def defaultDtor[V: ScalaReflection.universe.TypeTag]: Option[T => Option[V]] = None

  private final var index = -1
  final def createType[V: ScalaReflection.universe.TypeTag]
                      (ctor: Option[V => T] = defaultCtor[V], dtor: Option[T => Option[V]] = defaultDtor[V])
                      (implicit m: Manifest[V]): FlatUDTType[T, V] = {

    index += 1
    require(index < typesCount, s"You're trying to create more types than defined `typesCount` ($typesCount)")

    val finalCtor = ctor.getOrElse(throw new Exception(s"No ctor defined for udt type with index $index"))
    val finalDtor = dtor.getOrElse(throw new Exception(s"No dtor defined for udt type with index $index"))

    FlatUDTType[T, V](
      typesCount = typesCount,
      index = index,
      fromCatalystWrapper = fromCatalystWrapper,
      ctor = finalCtor,
      dtor = finalDtor
    )
  }
}
