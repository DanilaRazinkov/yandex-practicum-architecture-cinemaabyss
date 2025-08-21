FROM eclipse-temurin:21-jdk-alpine as builder
WORKDIR app
COPY . /app
RUN ./gradlew shadowJar

FROM eclipse-temurin:21-jre-alpine as runner
WORKDIR app
COPY --from=builder /app/build/libs/*-fat.jar /app/app.jar

CMD ["java", "-jar", "/app/app.jar"]
