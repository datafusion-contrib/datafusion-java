# datafusion-java

[![Build](https://github.com/G-Research/datafusion-java/actions/workflows/build.yml/badge.svg)](https://github.com/G-Research/datafusion-java/actions/workflows/build.yml)
[![Release](https://github.com/G-Research/datafusion-java/actions/workflows/release.yml/badge.svg)](https://github.com/G-Research/datafusion-java/actions/workflows/release.yml)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Fuk%2Fco%2Fgresearch%2Fdatafusion%2Fdatafusion-java%2Fmaven-metadata.xml)](https://repo1.maven.org/maven2/uk/co/gresearch/datafusion/datafusion-java/)

A Java binding to [Apache Arrow DataFusion][1]

## Status

This project is still work in progress, and currently it works with Arrow 9.0 and DataFusion 7.0 version.
It is build and verified in CI against Java 11 and 17. You may check out the docker run instructions
where Java 17 `jshell` is used to run interactively.

## How to use in your code

The artifacts are [published][2] to maven central, so you can use like any normal Java libraries:

```groovy
dependencies {
    implementation(
        group = "uk.co.gresearch.datafusion",
        name = "datafusion-java",
        version = "0.12.0" // or latest version, checkout https://github.com/G-Research/datafusion-java/releases
    )
}
```

To test it out, you can use this piece of demo code:

<details>
<summary>DataFusionDemo.java</summary>

```java
package com.me;

import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.SessionContexts;

public class DataFusionDemo {

    public static void main(String[] args) throws Exception {
        try (ExecutionContext executionContext = ExecutionContexts.create()) {
            executionContext.sql("select sqrt(65536)").thenCompose(DataFrame::show).join();
        }
    }
}
```

</details>

<details>
<summary>build.gradle.kts</summary>

```kotlin
plugins {
  java
  application
}

repositories {
  mavenCentral()
  google()
}

tasks {
  application {
    mainClass.set("com.me.DataFusionDemo")
  }
}

dependencies {
  implementation(
    group = "uk.co.gresearch.datafusion",
    name = "datafusion-java",
    version = "0.12.0"
  )
}

```

</details>

<details>
<summary>Run result</summary>

```

$ ./gradlew run
...
> Task :compileKotlin UP-TO-DATE
> Task :compileJava UP-TO-DATE
> Task :processResources NO-SOURCE
> Task :classes UP-TO-DATE

> Task :run
successfully created tokio runtime
+--------------------+
| sqrt(Int64(65536)) |
+--------------------+
| 256                |
+--------------------+
successfully shutdown tokio runtime

BUILD SUCCESSFUL in 2s
3 actionable tasks: 1 executed, 2 up-to-date
16:43:34: Execution finished 'run'.


```

</details>

## How to run the interactive demo

### 1. Run using Docker (with `jshell`)

First build the docker image:

<details>
<summary>docker build -t datafusion-example .</summary>

```text
❯ docker build -t datafusion-example .
[+] Building 615.2s (14/14) FINISHED
 => [internal] load build definition from Dockerfile                                                     0.0s
 => => transferring dockerfile: 598B                                                                     0.0s
 => [internal] load .dockerignore                                                                        0.0s
 => => transferring context: 81B                                                                         0.0s
 => [internal] load metadata for docker.io/library/openjdk:11-jdk-slim-bullseye                          5.6s
 => [internal] load build context                                                                       66.5s
 => => transferring context: 4.01GB                                                                     66.0s
 => [1/9] FROM docker.io/library/openjdk:11-jdk-slim-bullseye@sha256:0aac7dafc37d192d744228a6b26437438  22.3s
 => => resolve docker.io/library/openjdk:11-jdk-slim-bullseye@sha256:0aac7dafc37d192d744228a6b264374389  0.0s
 => => sha256:0aac7dafc37d192d744228a6b26437438908929883fc156b761ab779819e0fbd 549B / 549B               0.0s
 => => sha256:452daa20005a0f380b34b3d71a89e06cd7007086945fe3434d2a30fc1002475c 1.16kB / 1.16kB           0.0s
 => => sha256:7c8c5acc99dd425bd4b9cc46edc6f8b1fc7abd23cd5ea4c83d622d8ae1f2230f 5.60kB / 5.60kB           0.0s
 => => sha256:214ca5fb90323fe769c63a12af092f2572bf1c6b300263e09883909fc865d260 31.38MB / 31.38MB         2.6s
  1 update dockerfile, fix library path
 => => sha256:ebf31789c5c1a5e3676cbd7a34472d61217c52c819552f5b116565c22cb6d2f1 1.58MB / 1.58MB           2.3s
 => => sha256:8741521b2ba4d4d676c7a992cb54627c0eb9fdce1b4f68ad17da4f8b2abf103a 211B / 211B               2.5s
 => => sha256:2b079b63f250d1049457d0657541b735a1915d4c4a5aa6686d172c3821e3ebc9 204.24MB / 204.24MB      16.3s
 => => extracting sha256:214ca5fb90323fe769c63a12af092f2572bf1c6b300263e09883909fc865d260                2.7s
 => => extracting sha256:ebf31789c5c1a5e3676cbd7a34472d61217c52c819552f5b116565c22cb6d2f1                0.3s
 => => extracting sha256:8741521b2ba4d4d676c7a992cb54627c0eb9fdce1b4f68ad17da4f8b2abf103a                0.0s
 => => extracting sha256:2b079b63f250d1049457d0657541b735a1915d4c4a5aa6686d172c3821e3ebc9                5.9s
 => [2/9] RUN apt-get update &&   apt-get -y install curl gcc &&   rm -rf /var/lib/apt/lists/*          23.6s
 => [3/9] RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y                   29.6s
 => [4/9] WORKDIR /usr/opt/datafusion-java                                                               0.0s
 => [5/9] COPY build.gradle settings.gradle gradlew ./                                                   0.0s
 => [6/9] COPY gradle gradle                                                                             0.0s
 => [7/9] RUN ./gradlew --version                                                                        8.5s
 => [8/9] COPY . .                                                                                       8.9s
 => [9/9] RUN ./gradlew cargoReleaseBuild build installDist                                            494.7s
 => exporting to image                                                                                  21.9s
 => => exporting layers                                                                                 21.9s
 => => writing image sha256:36cabc4e6c400adb4fa0b10f9c07c79aa9b50703bc76a5727d3e43f85cc76f36             0.0s
 => => naming to docker.io/library/datafusion-example                                                    0.0s

Use '                                                                  0.0s
```

</details>

Then run using Docker:

<details>
<summary>docker run --rm -it datafusion-example</summary>

```text
Dec 27, 2021 2:52:22 AM java.util.prefs.FileSystemPreferences$1 run
INFO: Created user preferences directory.
|  Welcome to JShell -- Version 11.0.13
|  For an introduction type: /help intro

jshell> import org.apache.arrow.datafusion.*

jshell> var context = ExecutionContexts.create()
context ==> org.apache.arrow.datafusion.DefaultSessionContext@4229bb3f

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

</details>

### 2. Build from source

Note you must have local Rust and Java environment setup.

Run the example in one line:

```bash
./gradlew run
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

[1]: https://github.com/apache/arrow-datafusion
[2]: https://central.sonatype.com/artifact/uk.co.gresearch.datafusion/datafusion-java/
