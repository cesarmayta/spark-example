# levantar spark
docker compose up -d
# ingresar al worker
docker compose ps
# ejectuar el shell
docker exec -it <id> bash
# ir al ejecuta de spark
cd /opt/bitnami/spark/bin
# ejecutar spark
./spark-submit /opt/spark/prueba01.py