# datafusion-java

[![Build](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/build.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/build.yml)
[![Release](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/release.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-java/actions/workflows/release.yml)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Frepo.maven.apache.org%2Fmaven2%2Fio%2Fgithub%2Fdatafusion-contrib%2Fdatafusion-java%2Fmaven-metadata.xml)](https://repo.maven.apache.org/maven2/io/github/datafusion-contrib/datafusion-java/)

A Java binding to [Apache Arrow DataFusion][1]

## Status

This project is still work in progress, and currently it works with Arrow 9.0 and DataFusion 7.0 version.
It is build and verified in CI against Java 11 and 17. You may check out the docker run instructions
where Java 17 `jshell` is used to run interactively.

## How to use in your code

The artifacts are [published][1] to maven central, so you can use like any normal Java libraries:

```groovy
dependencies {
    implementation(
        group = "io.github.datafusion-contrib",
        name = "datafusion-java",
        version = "0.7.1" // or latest version, checkout https://github.com/datafusion-contrib/datafusion-java/releases
    )
}
```

Additionally, given this is a JNI project, you'll need to download the pre-built binary to be loaded during runtime. The pre-built libraries are compiled, signed, and verified from GitHub workflow actions, and are made available in https://repo.maven.apache.org/maven2/io/github/datafusion-contrib/datafusion-java/{a.b.c}/ where `{a.b.c}` is the latest version:

- `datafusion-java-a.b.c.so` is for linux-x86_64 machines
- `datafusion-java-a.b.c.dylib` is for macOS machines

Additionally you are encouraged to check the GPG signature as well as the sha256 sum of the binaries just to be sure.

Once downloaded, rename the library as `libdatafusion_jni.so` or `libdatafusion_jni.dylib` and put it to a directory readable by your application. During startup time, make sure you pass:

```bash
# or use gradle run but supply --args instead
java -Djava.library.path=/Users/me/dir/to/jni/library/ ...
```

to the command line.

To test it out, you can use this piece of demo code:

<details>
<summary>DataFusionDemo.java</summary>

```java
package com.me;

import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ExecutionContext;
import org.apache.arrow.datafusion.ExecutionContexts;

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
    applicationDefaultJvmArgs += "-Djava.library.path=/Users/me/libraries/"
  }
}

dependencies {
  implementation(
    group = "io.github.datafusion-contrib",
    name = "datafusion-java",
    version = "0.7.1"
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
<summary>docker build -t datafusion-java .</summary>

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
 => CACHED [stage-2 2/4] WORKDIR /usr/opt/datafusion-java                                                            0.0s
 => CACHED [rust-builder 2/6] RUN apt-get update &&   apt-get -y install curl gcc &&   rm -rf /var/lib/apt/lists/*   0.0s
 => CACHED [rust-builder 3/6] RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y            0.0s
 => CACHED [rust-builder 4/6] COPY datafusion-jni /usr/opt/datafusion-jni                                            0.0s
 => CACHED [rust-builder 5/6] WORKDIR /usr/opt/datafusion-jni                                                        0.0s
 => CACHED [rust-builder 6/6] RUN cargo build --release                                                              0.0s
 => CACHED [stage-2 3/4] COPY --from=rust-builder /usr/opt/datafusion-jni/target/release/libdatafusion_jni.so ./     0.0s
 => CACHED [java-builder 2/7] WORKDIR /usr/opt/datafusion-java                                                       0.0s
 => CACHED [java-builder 3/7] COPY build.gradle settings.gradle gradlew ./                                           0.0s
 => CACHED [java-builder 4/7] COPY gradle gradle                                                                     0.0s
 => CACHED [java-builder 5/7] RUN ./gradlew --version                                                                0.0s
 => CACHED [java-builder 6/7] COPY . .                                                                               0.0s
 => CACHED [java-builder 7/7] RUN ./gradlew installDist                                                              0.0s
 => CACHED [stage-2 4/4] COPY --from=java-builder /usr/opt/datafusion-java/datafusion-examples/build/install/datafu  0.0s
 => exporting to image                                                                                               0.0s
 => => exporting layers                                                                                              0.0s
 => => writing image sha256:eea330a6e9e2be4ac855ed31bdd1c81d52cd5e102e9fadce18ebed7e4104e87e                         0.0s
 => => naming to docker.io/library/datafusion-java                                                                   0.0s
```

</details>

Then run using Docker:

<details>
<summary>docker run --rm -it datafusion-java</summary>

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
[2]: https://repo.maven.apache.org/maven2/io/github/datafusion-contrib/datafusion-java/
