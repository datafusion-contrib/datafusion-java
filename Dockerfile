# build jni
FROM debian:bullseye as rust-builder

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
  apt-get -y install curl gcc && \
  rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

COPY datafusion_jni /usr/opt/datafusion_jni

WORKDIR /usr/opt/datafusion_jni

RUN cargo build --release

# build java
FROM openjdk:11-jdk-bullseye AS java-builder

WORKDIR /usr/opt/datafusion_java

COPY build.gradle settings.gradle gradlew ./

COPY gradle gradle

RUN ./gradlew --version

COPY . .

RUN ./gradlew shadowJar

# build shadow jar
FROM openjdk:11-jdk-slim-bullseye

WORKDIR /usr/opt/datafusion_java

COPY --from=rust-builder /usr/opt/datafusion_jni/target/release/libdatafusion_jni.so ./

COPY --from=java-builder /usr/opt/datafusion_java/datafusion_examples/build/libs/datafusion_examples-0.6-all.jar ./

CMD ["--class-path", "/usr/opt/datafusion_java/datafusion_examples-0.6-all.jar", "-R", "-Djava.library.path=/usr/opt/datafusion_java"]

ENTRYPOINT ["jshell"]
