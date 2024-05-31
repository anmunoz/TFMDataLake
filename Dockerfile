# Usa una imagen base con Maven y Java
FROM maven:3.8.1-openjdk-11

# Crea el directorio app
RUN mkdir -p /app

# Copia el archivo pom.xml al contenedor
COPY pom.xml /app

# Copia el directorio de código fuente al contenedor
COPY src /app/src

# Establece el directorio de trabajo
WORKDIR /app

# Compila el proyecto
RUN mvn compile

# Ejecuta el script de Scala
CMD ["mvn", "exec:java", "-Dexec.mainClass=org.tfmupm.ambulatoryReaderDocker"]