FROM jupyter/pyspark-notebook:latest

# Align PySpark with the Spark distribution in this image (Spark 3.5.0)
RUN pip uninstall -y pyspark py4j || true \
 && pip install --no-cache-dir pyspark==3.5.0
