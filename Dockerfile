# ========== ETAPA 1: COMPILAR ==========
# Imagen con Maven + JDK 17 solo para construir el .jar (no viaja a la imagen final)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Primero solo el pom: Docker reutiliza esta capa (dependencias descargadas) si el pom no cambia
COPY pom.xml .
RUN mvn -q dependency:go-offline

COPY src ./src
RUN mvn -q clean package -DskipTests

# ========== ETAPA 2: EJECUTAR ==========
# Imagen liviana: solo el JRE 17 y el .jar
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

EXPOSE 8082

# MaxRAMPercentage: la JVM usa hasta el 75% de la memoria asignada al contenedor
ENTRYPOINT ["sh", "-c", "java -XX:MaxRAMPercentage=75 $JAVA_OPTS -jar app.jar"]
