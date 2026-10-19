ARG MAVEN_IMAGE=maven:3.9.11-eclipse-temurin-21
ARG JAVA_IMAGE=eclipse-temurin:21-jre
FROM ${MAVEN_IMAGE} AS build
WORKDIR /build
COPY pom.xml .
COPY src src
RUN --mount=type=secret,id=maven_extra_ca \
    if [ -s /run/secrets/maven_extra_ca ]; then \
      keytool -importcert -noprompt -cacerts -storepass changeit \
        -alias demo-build-extra-ca -file /run/secrets/maven_extra_ca; \
    fi && mvn -B -q package

FROM ${JAVA_IMAGE}
WORKDIR /app
COPY --from=build /build/target/demo.jar /app/demo.jar
USER 10001:10001
EXPOSE 8080 8081
HEALTHCHECK --interval=5s --timeout=5s --start-period=20s --retries=30 CMD ["java","-jar","/app/demo.jar","--healthcheck"]
ENTRYPOINT ["java","-jar","/app/demo.jar"]
