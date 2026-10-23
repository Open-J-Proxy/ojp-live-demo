FROM eclipse-temurin:25-jdk
RUN mkdir /work && chown 65534:65534 /work
WORKDIR /work
COPY load/*.java /app/
USER 65534:65534
ENTRYPOINT ["java", "/app/Runner.java"]
