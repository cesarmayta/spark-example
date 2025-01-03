# Imagen
FROM docker.io/bitnami/spark:3.3.1

# Instala wget y unzip
USER root
RUN apt-get update && apt-get install -y wget unzip
