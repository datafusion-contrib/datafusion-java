# datafusion-java

[![Java CI](https://github.com/Jimexist/datafusion-java/actions/workflows/java.yml/badge.svg)](https://github.com/Jimexist/datafusion-java/actions/workflows/java.yml) [![Rust CI](https://github.com/Jimexist/datafusion-java/actions/workflows/rust.yml/badge.svg)](https://github.com/Jimexist/datafusion-java/actions/workflows/rust.yml)

A Java binding to [Apache Arrow Datafusion][1]

## Status

This project is still work in progress. Please check back later.

## How to run

Run the example in one line

```bash
./gradlew cargoBuild run
```

Or roll your own test example:

```java
// public class ExampleMain {
public static void main(String[] args) throws Exception {
  try (ExecutionContext context = ExecutionContexts.create();
      BufferAllocator allocator = new RootAllocator()) {
    DataFrame dataFrame = context.sql("select 1.5 + sqrt(2.0)");
    dataFrame.collect(allocator).thenAccept(ExampleMain::onReaderResult);
  }
}

private void onReaderResult(ArrowReader reader) {
  try {
    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    Schema schema = root.getSchema();
    while (reader.loadNextBatch()) {
      Float8Vector vector = (Float8Vector) root.getVector(0);
      for (int i = 0; i < root.getRowCount(); i += 1) {
        logger.info("value {}={}", i, vector.getValueAsDouble(i));
      }
    }
    // close to release resource
    reader.close();
  } catch (IOException e) {
    logger.warn("got IO Exception", e);
  }
}
// } /* end of ExampleMain */
```

To build the library:

```bash
./gradlew build
```

## How to develop

To Generate JNI Headers:

```bash
./gradlew generateJniHeaders
```

To Generate Javadoc:

```bash
./gradlew javadoc
```

To formate code (before push):

```bash
./gradlew spotlessApply # use spotlessCheck to check
```

[1]: https://github.com/apache/arrow-datafusion
