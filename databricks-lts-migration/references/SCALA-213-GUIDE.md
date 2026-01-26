# Scala 2.13 Migration Guide for Databricks

This guide covers migrating Scala code from 2.12 to 2.13 for Databricks Runtime 16.4 LTS and later.

---

## Overview

DBR 16.4 LTS introduces Scala 2.13 as the default. This is a **CRITICAL** migration that requires code changes for most Scala applications.

**Key Breaking Areas:**
1. Collection library redesign
2. Hash algorithm changes
3. Stricter type inference
4. Syntax deprecations

---

## Collection Library Changes

### Import Changes

| Scala 2.12 | Scala 2.13 |
|------------|------------|
| `import scala.collection.JavaConverters._` | `import scala.jdk.CollectionConverters._` |
| `collection.Seq` | `collection.immutable.Seq` |
| `TraversableOnce` | `IterableOnce` |
| `Traversable` | `Iterable` |

**Example Migration:**

```scala
// Scala 2.12
import scala.collection.JavaConverters._
import scala.collection.mutable

val javaList: java.util.List[String] = ...
val scalaSeq: Seq[String] = javaList.asScala.toSeq
val mutableBuffer: mutable.Buffer[String] = javaList.asScala

// Scala 2.13
import scala.jdk.CollectionConverters._
import scala.collection.mutable

val javaList: java.util.List[String] = ...
val scalaSeq: Seq[String] = javaList.asScala.toSeq
val mutableBuffer: mutable.Buffer[String] = javaList.asScala
```

### Collection Conversion Changes

| Scala 2.12 | Scala 2.13 |
|------------|------------|
| `.to[List]` | `.to(List)` |
| `.toIterator` | `.iterator` |
| `Stream` (lazy) | `LazyList` |
| `.view.force` | `.view.to(List)` |

**Example:**

```scala
// Scala 2.12
val list = seq.to[List]
val iter = seq.toIterator
val stream = Stream.from(1)

// Scala 2.13  
val list = seq.to(List)
val iter = seq.iterator
val lazyList = LazyList.from(1)
```

### Using Compatibility Library

Add to `build.sbt`:
```scala
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0"
```

Then add import:
```scala
import scala.collection.compat._
```

This provides backward-compatible APIs for many collection operations.

---

## Hash Algorithm Changes

### Problem

HashMap, HashSet, and other hash-based collections may iterate in different order between Scala 2.12 and 2.13.

### Detection

Look for code that:
- Assumes specific iteration order on HashMaps/HashSets
- Serializes collections without explicit ordering
- Compares collections by iteration order in tests

### Solution

```scala
// BAD: Relies on iteration order
val map = HashMap("a" -> 1, "b" -> 2, "c" -> 3)
map.foreach { case (k, v) => println(s"$k: $v") }  // Order may differ!

// GOOD: Explicit ordering when order matters
val map = HashMap("a" -> 1, "b" -> 2, "c" -> 3)
map.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"$k: $v") }

// GOOD: Use order-preserving collection
import scala.collection.immutable.ListMap
val orderedMap = ListMap("a" -> 1, "b" -> 2, "c" -> 3)

// GOOD: Use LinkedHashMap for insertion order
import scala.collection.mutable.LinkedHashMap
val linkedMap = LinkedHashMap("a" -> 1, "b" -> 2, "c" -> 3)
```

### For Tests

```scala
// BAD: Order-dependent assertion
assert(map.keys.toList == List("a", "b", "c"))

// GOOD: Order-independent assertion
assert(map.keys.toSet == Set("a", "b", "c"))
assert(map.keys.toList.sorted == List("a", "b", "c"))
```

---

## Type Inference Changes

### Stricter Inference

Scala 2.13 has stricter type inference. Code that compiled in 2.12 may need explicit type annotations.

**Example:**

```scala
// Scala 2.12 - compiles
val result = list.map(x => x.toString).fold("")(_ + _)

// Scala 2.13 - may need annotation
val result: String = list.map(x => x.toString).fold("")(_ + _)
```

### Variance Annotations

Some variance issues surface in 2.13:

```scala
// May need explicit types in 2.13
def process[A](items: Seq[A]): Seq[A] = {
  // explicit return type may be needed
  items.map(identity)
}
```

---

## Syntax Changes

### Single-Quote Strings Deprecated

```scala
// Scala 2.12 - works
val c = 'a'  // Character literal (still works)
val s = 'symbol  // Symbol literal (deprecated)

// Scala 2.13
val c = 'a'  // Character literal (still works)
val s = Symbol("symbol")  // Use Symbol constructor
```

### Postfix Operators Deprecated

```scala
// Scala 2.12 - works
val list = 1 to 10 toList

// Scala 2.13 - use dot notation
val list = (1 to 10).toList
```

### String Concatenation

```scala
// Scala 2.12 - works
val s = 1 + " item"  // Uses implicit conversion

// Scala 2.13 - explicit toString recommended
val s = 1.toString + " item"
// or use interpolation
val s = s"$1 item"
```

---

## Spark-Specific Considerations

### UDF Compilation

Scala UDFs must be recompiled for 2.13:

```scala
// Recompile for Scala 2.13
import org.apache.spark.sql.functions.udf

val myUdf = udf((value: String) => {
  // UDF logic - ensure 2.13 compatible
  value.toUpperCase
})
```

### DataFrame Operations

Some implicit behaviors change:

```scala
// Be explicit about collection types
val columns: Seq[String] = df.columns.toSeq  // Explicit Seq
val rows: Array[Row] = df.collect()  // Returns Array
```

### Third-Party Libraries

Ensure all dependencies are compiled for Scala 2.13:

```scala
// build.sbt - check Scala version suffix
libraryDependencies += "org.example" %% "library" % "1.0.0"
// %% automatically appends _2.13 for Scala 2.13 builds
```

---

## Migration Steps

### Step 1: Update Build Configuration

```scala
// build.sbt
scalaVersion := "2.13.10"

// Or for cross-compilation
crossScalaVersions := Seq("2.12.15", "2.13.10")
```

### Step 2: Update Dependencies

```scala
// Ensure all dependencies support 2.13
libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
  // Update other dependencies to 2.13-compatible versions
)
```

### Step 3: Fix Compilation Errors

1. Update imports (`JavaConverters` → `CollectionConverters`)
2. Fix collection conversions (`.to[List]` → `.to(List)`)
3. Add explicit type annotations where needed
4. Update deprecated syntax

### Step 4: Fix Runtime Issues

1. Review hash-dependent code
2. Test iteration order assumptions
3. Validate serialization compatibility

### Step 5: Test Thoroughly

1. Run all unit tests
2. Integration tests with Spark
3. Compare outputs between 2.12 and 2.13 builds

---

## Common Errors and Fixes

### Error: `value asScala is not a member of java.util.List`

**Fix:** Update import
```scala
// Change from
import scala.collection.JavaConverters._
// To
import scala.jdk.CollectionConverters._
```

### Error: `type mismatch; found: scala.collection.Seq, required: scala.collection.immutable.Seq`

**Fix:** Be explicit about collection type
```scala
val seq: scala.collection.immutable.Seq[String] = list.toSeq
// Or use .toList for immutable
val list: List[String] = items.toList
```

### Error: `value to is not a member of...`

**Fix:** Update `.to` syntax
```scala
// Change from
collection.to[List]
// To
collection.to(List)
```

### Warning: `postfix operator toList should be enabled`

**Fix:** Use dot notation
```scala
// Change from
1 to 10 toList
// To
(1 to 10).toList
```

---

## Resources

- [Scala 2.13 Migration Guide](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
- [Scala Collection Compatibility Library](https://github.com/scala/scala-collection-compat)
- [Databricks Runtime 16.4 Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/16.4lts)
