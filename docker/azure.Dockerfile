FROM python:3.10-slim

ENV SCRIPT dbt_server.py
ENV LOGGING_SERVICE AzureMonitor
ENV CLOUD_STORAGE_SERVICE AzureBlobStorage
ENV METADATA_DOCUMENT_SERVICE CosmosDB
ENV JOB_SERVICE ContainerAppsJob

RUN useradd -ms /bin/bash azure
RUN chown -R azure /home/azure/

WORKDIR /home/azure/
USER azure

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

RUN python -m poetry install --no-interaction --only azure

ADD src/ src/

CMD ["python", "-m", "poetry", "run", "python", "-m", "$SCRIPT"]
