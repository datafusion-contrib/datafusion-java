# datafusion-java

[![Java CI](https://github.com/Jimexist/datafusion-java/actions/workflows/java.yml/badge.svg)](https://github.com/Jimexist/datafusion-java/actions/workflows/java.yml) [![Rust CI](https://github.com/Jimexist/datafusion-java/actions/workflows/rust.yml/badge.svg)](https://github.com/Jimexist/datafusion-java/actions/workflows/rust.yml)

A Java binding to [Apache Arrow Datafusion][1]

## Status

This project is still work in progress. Please check back later.

## How to run

### 1. Run using Docker

First build the docker image:

```bash
docker build -t datafusion-java .
```

```text
[+] Building 276.7s (17/17) FINISHED
 => [internal] load build definition from Dockerfile                                                     0.0s
 => => transferring dockerfile: 890B                                                                     0.0s
 => [internal] load .dockerignore                                                                        0.0s
 => => transferring context: 2B                                                                          0.0s
 => [internal] load metadata for docker.io/library/openjdk:11.0.12-jdk-slim-bullseye                     1.4s
 => [internal] load metadata for docker.io/library/openjdk:11.0.12-jdk-bullseye                          1.4s
 => [internal] load build context                                                                        0.6s
 => => transferring context: 1.06MB                                                                      0.6s
 => [builder 1/7] FROM docker.io/library/openjdk:11.0.12-jdk-bullseye@sha256:7d7c3de8d1231e8910d163a6b3  0.0s
 => [stage-1 1/4] FROM docker.io/library/openjdk:11.0.12-jdk-slim-bullseye@sha256:e7cb0867beb749222b109  0.0s
 => CACHED [builder 2/7] RUN apt-get update &&   apt-get -y install gcc &&   rm -rf /var/lib/apt/lists/  0.0s
 => CACHED [builder 3/7] RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y     0.0s
 => CACHED [builder 4/7] WORKDIR /usr/opt/datafusion_java                                                0.0s
 => [builder 5/7] COPY . .                                                                               5.6s
 => [builder 6/7] RUN cd datafusion_jni && cargo build --release && cd ..                              179.3s
 => [builder 7/7] RUN ./gradlew build                                                                   89.0s
 => CACHED [stage-1 2/4] WORKDIR /usr/opt/datafusion_java                                                0.0s
 => CACHED [stage-1 3/4] COPY --from=builder /usr/opt/datafusion_java/datafusion_examples/build/libs/da  0.0s
 => [stage-1 4/4] COPY --from=builder /usr/opt/datafusion_java/datafusion_jni/target/release/libdatafus  0.1s
 => exporting to image                                                                                   0.1s
 => => exporting layers                                                                                  0.1s
 => => writing image sha256:4fee2427d99ef049d669a632fc49b399cea4d1ea214a14a010916648c12085ef             0.0s
 => => naming to docker.io/library/datafusion-java                                                       0.0s
```

Then run using Docker:

```bash
docker run --rm -it datafusion-java
```

```text
Oct 17, 2021 11:27:32 AM java.util.prefs.FileSystemPreferences$1 run
INFO: Created user preferences directory.
|  Welcome to JShell -- Version 11.0.12
|  For an introduction type: /help intro

jshell> import org.apache.arrow.datafusion.*

jshell> var context = ExecutionContexts.create()
11:27:52.555 [main] INFO  org.apache.arrow.datafusion.AbstractProxy - Obtaining DefaultExecutionContext@7fee741f4e30
11:27:52.558 [main] INFO  org.apache.arrow.datafusion.AbstractProxy - Obtaining TokioRuntime@7fee744d0680
context ==> org.apache.arrow.datafusion.DefaultExecutionContext@6babf3bf

jshell> var df = context.sql("select 1.1 + cos(2.0)")
11:28:43.573 [main] INFO  org.apache.arrow.datafusion.AbstractProxy - Obtaining DefaultDataFrame@7fee74140890
df ==> org.apache.arrow.datafusion.DefaultDataFrame@10feca44

jshell> import org.apache.arrow.memory.*

jshell> var allocator = new RootAllocator()
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
WARNING: sun.reflect.Reflection.getCallerClass is not supported. This will impact performance.
allocator ==> Allocator(ROOT) 0/0/0/9223372036854775807 (res/actual/peak/limit)


jshell> var r = df.collect(allocator).join()
11:29:41.621 [main] INFO  org.apache.arrow.datafusion.DefaultDataFrame - successfully completed with arr length=542
r ==> org.apache.arrow.vector.ipc.ArrowFileReader@6f15d60e

jshell> var root = r.getVectorSchemaRoot()
root ==> org.apache.arrow.vector.VectorSchemaRoot@2e11485

jshell> r.loadNextBatch()
$8 ==> true

jshell> var v = root.getVector(0)
v ==> [0.6838531634528577]
```

### 2. Using pre-built artifact

1. Checkout the [release page](https://github.com/Jimexist/datafusion-java/releases) for a pre-built MacOS JNI library.
1. Checkout the [GitHub maven repository](https://github.com/Jimexist/datafusion-java/packages/1047809) for installing the Java artifacts.

### 3. Build from source

Note you must have local Rust and Java environment setup.

Run the example in one line:

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
