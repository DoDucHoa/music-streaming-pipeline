# Stage 1: Build Stage
FROM sbtscala/scala-sbt:eclipse-temurin-25.0.1_8_1.12.0_2.13.18 AS build

# Set the working directory
WORKDIR /app

# Copy the build definition files first to leverage Docker layer caching
COPY build.sbt ./
COPY project ./project

# This "warm-up" step downloads dependencies without the source code
RUN sbt update

# Now copy the actual source code
COPY . .

RUN sbt "show Compile / packageBin / artifactPath"

# Package the application (skipping tests for speed, remove if desired)
RUN sbt clean assembly

# Stage 2: Runtime Stage
FROM eclipse-temurin:8-jre-jammy

WORKDIR /app

# Copy the fat JAR from the build stage
# Note: Adjust the path below to match your actual JAR name and location
COPY --from=build /app/target/scala-2.12/*.jar app.jar
COPY --from=build /app/examples /app/examples
COPY --from=build /app/data /app/data
COPY --from=build /app/configs /app/configs

# Expose the port your app runs on
# EXPOSE 8080

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]