# build jni
FROM debian:bullseye as rust-builder

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
  apt-get -y install curl gcc && \
  rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

COPY datafusion-jni /usr/opt/datafusion-jni

WORKDIR /usr/opt/datafusion-jni

RUN cargo build --release

# build java
FROM openjdk:17-jdk-slim-bullseye AS java-builder

WORKDIR /usr/opt/datafusion-java

COPY build.gradle settings.gradle gradlew ./

COPY gradle gradle

RUN ./gradlew --version

COPY . .

RUN ./gradlew installDist

FROM openjdk:17-jdk-slim-bullseye

WORKDIR /usr/opt/datafusion-java

COPY --from=rust-builder /usr/opt/datafusion-jni/target/release/libdatafusion_jni.so ./

COPY --from=java-builder /usr/opt/datafusion-java/datafusion-examples/build/install/datafusion-examples ./

CMD ["--class-path", "/usr/opt/datafusion-java/lib/*", "-R", "-Djava.library.path=/usr/opt/datafusion-java"]

ENTRYPOINT ["jshell"]
