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

FROM openjdk:11-jdk-bullseye AS java-builder

COPY . /usr/opt/datafusion_java

WORKDIR /usr/opt/datafusion_java

RUN ./gradlew shadowJar

FROM openjdk:11-jdk-slim-bullseye

WORKDIR /usr/opt/datafusion_java

ENV DATAFUSION_VERISON=0.3-SNAPSHOT

COPY --from=rust-builder /usr/opt/datafusion_jni/target/release/libdatafusion_jni.so ./

COPY --from=java-builder /usr/opt/datafusion_java/datafusion_examples/build/libs/datafusion_examples-${DATAFUSION_VERISON}-all.jar ./

CMD ["--class-path", "/usr/opt/datafusion_java/datafusion_examples-${DATAFUSION_VERISON}-all.jar", "-R", "-Djava.library.path=/usr/opt/datafusion_java"]

ENTRYPOINT ["jshell"]
