FROM openjdk:11-jdk-slim-bullseye

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
  apt-get -y install curl gcc && \
  rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /usr/opt/datafusion-java

COPY build.gradle settings.gradle gradlew ./

COPY gradle gradle

RUN ./gradlew --version

COPY . .

RUN ./gradlew copyDevLibrary installDist

CMD ["./datafusion-examples/build/install/datafusion-examples/bin/datafusion-examples"]
