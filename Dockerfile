FROM openjdk:11-jdk-bullseye AS builder

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
  apt-get -y install gcc && \
  rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /usr/opt/datafusion_java

COPY . .

RUN cd datafusion_jni && cargo build --release && cd ..

RUN ./gradlew build

FROM openjdk:11-jdk-slim-bullseye

WORKDIR /usr/opt/datafusion_java

COPY --from=builder /usr/opt/datafusion_java/datafusion_examples/build/libs/datafusion_examples-1.0-all.jar ./

COPY --from=builder /usr/opt/datafusion_java/datafusion_jni/target/release/libdatafusion_jni.so ./

CMD ["--class-path", "/usr/opt/datafusion_java/datafusion_examples-1.0-all.jar", "-R", "-Djava.library.path=/usr/opt/datafusion_java"]

ENTRYPOINT ["jshell"]
