# adds mongodb to spark
FROM gcr.io/spark-operator/spark-py:v3.1.1

RUN pip install pymongo

ARG root_password
ENV MONGODB_ROOT_PASSWORD $root_password
