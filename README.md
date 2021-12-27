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
[+] Building 3.6s (24/24) FINISHED
 => [internal] load build definition from Dockerfile                                                                 0.0s
 => => transferring dockerfile: 37B                                                                                  0.0s
 => [internal] load .dockerignore                                                                                    0.0s
 => => transferring context: 34B                                                                                     0.0s
 => [internal] load metadata for docker.io/library/openjdk:11-jdk-slim-bullseye                                      3.3s
 => [internal] load metadata for docker.io/library/debian:bullseye                                                   1.5s
 => [internal] load metadata for docker.io/library/openjdk:11-jdk-bullseye                                           0.0s
 => [internal] load build context                                                                                    0.1s
 => => transferring context: 599.56kB                                                                                0.1s
 => [rust-builder 1/6] FROM docker.io/library/debian:bullseye@sha256:2906804d2a64e8a13a434a1a127fe3f6a28bf7cf3696be  0.0s
 => [java-builder 1/7] FROM docker.io/library/openjdk:11-jdk-bullseye                                                0.0s
 => [stage-2 1/4] FROM docker.io/library/openjdk:11-jdk-slim-bullseye@sha256:5d1529573ab358fd46b823459bae966ca763ed  0.0s
 => CACHED [stage-2 2/4] WORKDIR /usr/opt/datafusion_java                                                            0.0s
 => CACHED [rust-builder 2/6] RUN apt-get update &&   apt-get -y install curl gcc &&   rm -rf /var/lib/apt/lists/*   0.0s
 => CACHED [rust-builder 3/6] RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y            0.0s
 => CACHED [rust-builder 4/6] COPY datafusion_jni /usr/opt/datafusion_jni                                            0.0s
 => CACHED [rust-builder 5/6] WORKDIR /usr/opt/datafusion_jni                                                        0.0s
 => CACHED [rust-builder 6/6] RUN cargo build --release                                                              0.0s
 => CACHED [stage-2 3/4] COPY --from=rust-builder /usr/opt/datafusion_jni/target/release/libdatafusion_jni.so ./     0.0s
 => CACHED [java-builder 2/7] WORKDIR /usr/opt/datafusion_java                                                       0.0s
 => CACHED [java-builder 3/7] COPY build.gradle settings.gradle gradlew ./                                           0.0s
 => CACHED [java-builder 4/7] COPY gradle gradle                                                                     0.0s
 => CACHED [java-builder 5/7] RUN ./gradlew --version                                                                0.0s
 => CACHED [java-builder 6/7] COPY . .                                                                               0.0s
 => CACHED [java-builder 7/7] RUN ./gradlew installDist                                                              0.0s
 => CACHED [stage-2 4/4] COPY --from=java-builder /usr/opt/datafusion_java/datafusion_examples/build/install/datafu  0.0s
 => exporting to image                                                                                               0.0s
 => => exporting layers                                                                                              0.0s
 => => writing image sha256:eea330a6e9e2be4ac855ed31bdd1c81d52cd5e102e9fadce18ebed7e4104e87e                         0.0s
 => => naming to docker.io/library/datafusion-java                                                                   0.0s
```

Then run using Docker:

```bash
docker run --rm -it datafusion-java
```

```text
Dec 27, 2021 2:52:22 AM java.util.prefs.FileSystemPreferences$1 run
INFO: Created user preferences directory.
|  Welcome to JShell -- Version 11.0.13
|  For an introduction type: /help intro

jshell> import org.apache.arrow.datafusion.*

jshell> var context = ExecutionContexts.create()
context ==> org.apache.arrow.datafusion.DefaultExecutionContext@4229bb3f

jshell> var df = context.sql("select 1.1 + cos(2.0)").join()
df ==> org.apache.arrow.datafusion.DefaultDataFrame@1a18644

jshell> import org.apache.arrow.memory.*

jshell> var allocator = new RootAllocator()
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
allocator ==> Allocator(ROOT) 0/0/0/9223372036854775807 (res/actual/peak/limit)


jshell> var r = df.collect(allocator).join()
02:52:46.882 [main] INFO  org.apache.arrow.datafusion.DefaultDataFrame - successfully completed with arr length=538
r ==> org.apache.arrow.vector.ipc.ArrowFileReader@5167f57d

jshell> var root = r.getVectorSchemaRoot()
root ==> org.apache.arrow.vector.VectorSchemaRoot@4264b240

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
