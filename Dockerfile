FROM amazoncorretto:21

RUN yum install -y gcc && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /usr/opt/datafusion-java

COPY build.gradle settings.gradle gradlew ./

COPY gradle gradle

RUN ./gradlew --version

COPY . .

RUN ./gradlew copyDevLibrary installDist

# Set working directory so that the relative paths to resource files used in ExampleMain are correct
WORKDIR /usr/opt/datafusion-java/datafusion-examples

# Configure environment variables to allow loading datafusion-java in jshell
ENV CLASSPATH="/usr/opt/datafusion-java/datafusion-examples/build/install/datafusion-examples/lib/*"
ENV JDK_JAVA_OPTIONS="-Djava.library.path=/usr/opt/datafusion-java/datafusion-java/build/jni_libs/dev --add-opens=java.base/java.nio=ALL-UNNAMED"

CMD ["./build/install/datafusion-examples/bin/datafusion-examples"]
