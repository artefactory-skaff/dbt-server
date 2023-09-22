FROM python:3.10-slim

ENV SCRIPT dbt_server.py
ENV LOGGING_SERVICE GoogleCloudLogging
ENV CLOUD_STORAGE_SERVICE GoogleCloudStorage
ENV METADATA_DOCUMENT_SERVICE Firestore
ENV JOB_SERVICE CloudRunJob

RUN useradd -ms /bin/bash gcp
RUN chown -R gcp /home/gcp/

WORKDIR /home/gcp/
USER gcp

RUN pip install poetry

RUN mkdir package/
RUN touch package/__init__.py
RUN touch package/README.md
RUN mkdir api/
RUN touch api/__init__.py
COPY pyproject.toml ./
COPY poetry.lock ./
RUN mkdir seeds

RUN python -m poetry install --no-interaction --only gcp

ADD api/ api/

CMD ["python", "-m", "poetry", "run", "python", "-m", "$SCRIPT"]
