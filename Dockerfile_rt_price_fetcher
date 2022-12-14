FROM python:3.9

ARG root_password
WORKDIR /app
COPY ./realtime-price-fetcher .
COPY ./constants.py .
RUN pip install -r requirements.txt
ENV MONGODB_ROOT_PASSWORD $root_password
