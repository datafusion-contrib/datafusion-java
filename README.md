# datafusion-java

[![Java CI](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/java.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/java.yml) [![Rust CI](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/rust.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/rust.yml)

A Java binding to [Apache Arrow Datafusion][1]

## Status

This project is still work in progress, and currently it works with Arrow 6.0 and Datafusion 6.0 version.
It is build and verified in CI against Java 8, 11, and 15. You may check out the docker run instructions
where Java 11 `jshell` is used to run interactively.

## How to run

### 1. Run using Docker (with `jshell`)

First build the docker image:

```bash
docker build -t datafusion-java .
```

```text
❯ docker build -t datafusion-java .
[+] Building 101.2s (24/24) FINISHED
 => [internal] load build definition from Dockerfile                                                     0.0s
 => => transferring dockerfile: 37B                                                                      0.0s
 => [internal] load .dockerignore                                                                        0.0s
 => => transferring context: 34B                                                                         0.0s
 => [internal] load metadata for docker.io/library/openjdk:11-jdk-slim-bullseye                          1.6s
 => [internal] load metadata for docker.io/library/debian:bullseye                                       0.0s
 => [internal] load metadata for docker.io/library/openjdk:11-jdk-bullseye                               1.7s
 => [stage-2 1/4] FROM docker.io/library/openjdk:11-jdk-slim-bullseye@sha256:ad41c90d47fdc84fecb3bdba2d  0.0s
 => [internal] load build context                                                                        0.4s
 => => transferring context: 714.37kB                                                                    0.4s
 => [java-builder 1/7] FROM docker.io/library/openjdk:11-jdk-bullseye@sha256:81cce461e2ac37d6f557f57ff8  0.0s
 => [rust-builder 1/6] FROM docker.io/library/debian:bullseye                                            0.0s
 => CACHED [java-builder 2/7] WORKDIR /usr/opt/datafusion_java                                           0.0s
 => CACHED [java-builder 3/7] COPY build.gradle settings.gradle gradlew ./                               0.0s
 => CACHED [java-builder 4/7] COPY gradle gradle                                                         0.0s
 => CACHED [java-builder 5/7] RUN ./gradlew --version                                                    0.0s
 => [java-builder 6/7] COPY . .                                                                          4.2s
 => [java-builder 7/7] RUN ./gradlew shadowJar                                                          94.6s
 => CACHED [stage-2 2/4] WORKDIR /usr/opt/datafusion_java                                                0.0s
 => CACHED [rust-builder 2/6] RUN apt-get update &&   apt-get -y install curl gcc &&   rm -rf /var/lib/  0.0s
 => CACHED [rust-builder 3/6] RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s --   0.0s
 => CACHED [rust-builder 4/6] COPY datafusion_jni /usr/opt/datafusion_jni                                0.0s
 => CACHED [rust-builder 5/6] WORKDIR /usr/opt/datafusion_jni                                            0.0s
 => CACHED [rust-builder 6/6] RUN cargo build --release                                                  0.0s
 => CACHED [stage-2 3/4] COPY --from=rust-builder /usr/opt/datafusion_jni/target/release/libdatafusion_  0.0s
 => [stage-2 4/4] COPY --from=java-builder /usr/opt/datafusion_java/datafusion_examples/build/libs/data  0.0s
 => exporting to image                                                                                   0.1s
 => => exporting layers                                                                                  0.1s
 => => writing image sha256:deacd5b3bd1ceba2c0ae0060e49bb148e2468fd355870657679a3566abb725de             0.0s
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

jshell> var df = context.sql("select 1.1 + cos(2.0)").join()
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

1. Checkout the [release page](https://github.com/datafusion-contrib/datafusion-java/releases) for a pre-built MacOS JNI library.
1. Checkout the [GitHub maven repository](https://github.com/datafusion-contrib/datafusion-java/packages/1047809) for installing the Java artifacts.

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
