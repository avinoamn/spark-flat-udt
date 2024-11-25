# Flat UDT for Apache Spark

This project implements a flat **User-Defined Type (UDT)** framework for Apache Spark. It simplifies working with complex types in Spark SQL by flattening them into structured formats, enabling efficient serialization and deserialization into Catalyst's internal representation.


## Table of Contents
1. [Installation](#installation)
2. [Features](#features)
3. [Code Overview](#code-overview)
4. [Usage Example](#usage-example)
    - [Step 1: Define the Factory](#step-1-define-the-factory)
    - [Step 2: Define the UDT](#step-2-define-the-udt)
    - [Step 3: Register and Use the UDT](#step-3-register-and-use-the-udt)
5. [Schema Representation](#schema-representation)
6. [How It Works](#how-it-works)
7. [Key Components](#key-components)


## Installation

To include this library in your project, clone this repository and install it locally.

For Maven users, add the following dependency to your `pom.xml`:

```xml
<dependency>
   <groupId>github.avinoamn</groupId>
   <artifactId>spark-flat-udt</artifactId>
   <version>1.0.0</version>
   <scope>compile</scope>
</dependency>
```


## Features

- **Flattened Representation**: Converts nested types into flat Spark SQL schemas for easier manipulation.
- **Custom Serialization**: Full control over serialization and deserialization of complex Scala/Java types.
- **Ease of Use**: Designed to simplify UDT definitions for intricate types, such as nested `Either` constructs.


## Code Overview

#### The library introduces the following components:

1. `FlatUDTType`: Represents a single field in a `FlatUDT`.
2. `FlatUDTTypeFactory`: Factory for creating `FlatUDTType` instances.
3. `FlatUDT`: Abstract class that aggregates multiple `FlatUDTType` instances, defining schemas and handling serialization / deserialization.


## Usage Example

Let's walk through how to use this library with a complex type:

```scala
type Property = Either[Either[String, Boolean], Either[Long, Double]]
```

We will create a UDT to handle this type in three steps.


### Step 1: Define the Factory

Create a factory to generate `FlatUDTType` instances for each supported type in `Property`.

```scala
import github.avinoamn.spark.sql.UDT.flat.{FlatUDTType, FlatUDTTypeFactory}

import org.apache.spark.sql.catalyst.ScalaReflection

class PropertyTypeFactory extends FlatUDTTypeFactory[Property] {
  override val typesCount: Int = 4

  override def defaultCtor[V: ScalaReflection.universe.TypeTag]: Option[V => Property] = Some {
    case v: String  => Left(Left(v))
    case v: Boolean => Left(Right(v))
    case v: Long    => Right(Left(v))
    case v: Double  => Right(Right(v))
  }

  override def defaultDtor[V: ScalaReflection.universe.TypeTag]: Option[Property => Option[V]] = Some {
    case Left(Left(v: V))  => Some(v)
    case Left(Right(v: V)) => Some(v)
    case Right(Left(v: V)) => Some(v)
    case Right(Right(v: V)) => Some(v)
    case _ => None
  }
}
```


### Step 2: Define the UDT

Create a UDT class that uses the factory to define a schema and serialization logic.

```scala
import github.avinoamn.spark.sql.UDT.flat.{FlatUDT, FlatUDTType}
import org.apache.spark.sql.types.{DataType, StructType}

class PropertyUDT extends FlatUDT[Property] {
  private val factory = new PropertyFactory

  override def types: Array[FlatUDTType[Property, _]] = Array(
    factory.createType[String](),
    factory.createType[Boolean](),
    factory.createType[Long](),
    factory.createType[Double]()
  )
}
```


### Step 3: Register and Use the UDT

Now, you can register and use the UDT in your Spark application.

```scala
object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FlatUDT Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    UDTRegistration.register(classOf[Property].getName, classOf[PropertyUDT].getName)

    // Sample data
    val data: Seq[Property] = Seq(
      Left(Left("example string")),
      Left(Right(true)),
      Right(Left(123L)),
      Right(Right(45.67))
    )

    // Convert data to Dataset
    val ds: Dataset[Property] = data.toDS()
    ds.show()

    // Serialize data using UDT
    val df = ds.toDF("property")
    df.printSchema()
    df.show()
  }
}
```


## Schema Representation
The `FlatUDT` framework represents the UDT as a `StructType`. For `Property`, the schema would look like so:

```text
StructType(
  StructField("typeIndex", StringType, true),
  StructField("0", StringType, true),
  StructField("1", BooleanType, true),
  StructField("2", LongType, true),
  StructField("3", DoubleType, true)
)
```

* `typeIndex`: Identifies the variant of the Either type.
* **Additional Fields**: One field per type variant.


## How It Works
1. **Type Mapping**: Maps Scala types to Spark SQL Catalyst types using `ScalaReflection`.

2. **Serialization**: Converts user-defined types into Catalyst's `InternalRow`.

3. **Deserialization**: Reconstructs user-defined types from Catalyst's `InternalRow`.
   
4. **Dynamic Schema**: Automatically generates a schema for the given types.


## Key Components

### `fromCatalystWrapper`

The `fromCatalystWrapper` function is a hook provided in the `FlatUDTTypeFactory` class to allow additional transformation or processing of values when deserializing data from Catalyst's internal format.

**Why is it needed?**

- Spark's Catalyst engine uses its own optimized format for data processing, which may not directly map to Scala types.

- Some complex types (e.g., custom objects or nested data) might require additional conversion logic after being deserialized from Catalyst format.

**How does it work?**

- It takes the result of Spark's internal deserialization (`fromCatalyst`) and applies any custom logic defined by the user.
- By default, it returns the value unchanged, but users can override it to provide custom handling.

**Example**

If the data stored in Catalyst format is a `UTF8String`, you can convert it to a Scala `String` during deserialization using `fromCatalystWrapper`:

```scala
override def fromCatalystWrapper(fromCatalyst: Any): Any = {
  fromCatalyst match {
    case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
    case other => other
  }
}
```

This allows you to adapt Catalyst's internal representations to your application's requirements seamlessly.