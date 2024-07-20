FROM "maven:3.9.8-eclipse-temurin-22"

COPY . .
RUN mvn install