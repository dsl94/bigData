FROM bde2020/spark-python-template:3.1.2-hadoop3.2

COPY main.py /app/

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main.py
ENV SPARK_APPLICATION_ARGS --station 564 --from_date 2022-01-01T00:00:00 --to_date 2022-01-02T00:00:00
#ENV SPARK_SUBMIT_ARGS --executor-memory 4 --executor-cores 2