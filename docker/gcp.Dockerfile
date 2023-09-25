FROM python:3.10-slim

ENV SCRIPT dbt_server.py
ENV LOGGING_SERVICE GoogleCloudLogging
ENV STORAGE_SERVICE GoogleCloudStorage
ENV METADATA_DOCUMENT_SERVICE Firestore
ENV JOB_SERVICE CloudRunJob

RUN useradd -ms /bin/bash gcp
RUN chown -R gcp /home/gcp/

WORKDIR /home/gcp/
USER gcp

RUN pip install poetry

COPY pyproject.toml ./
COPY poetry.lock ./
RUN mkdir seeds
RUN mkdir src
RUN mkdir src/dbt_remote
RUN mkdir src/dbt_server
RUN touch src/dbt_remote/__init__.py
RUN touch src/dbt_server/__init__.py
RUN touch README.md

RUN python -m poetry install --no-interaction --only gcp

ADD src/ src/

CMD ["python", "-m", "poetry", "run", "python", "-m", "$SCRIPT"]
