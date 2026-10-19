ARG MAVEN_IMAGE=maven:3.9.11-eclipse-temurin-21
ARG JAVA_IMAGE=eclipse-temurin:21-jre
FROM ${MAVEN_IMAGE} AS build
WORKDIR /build
COPY pom.xml .
COPY src src
RUN mvn -B -q package

FROM ${JAVA_IMAGE}
WORKDIR /app
COPY --from=build /build/target/demo.jar /app/demo.jar
USER 10001:10001
EXPOSE 8080 8081
ENTRYPOINT ["java","-jar","/app/demo.jar"]
