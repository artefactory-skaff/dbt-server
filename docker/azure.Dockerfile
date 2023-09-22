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

RUN mkdir package/
RUN touch package/__init__.py
RUN touch package/README.md
RUN mkdir api/
RUN touch api/__init__.py
COPY pyproject.toml ./
COPY poetry.lock ./
RUN mkdir seeds

RUN python -m poetry install --no-interaction --only azure

ADD api/ api/

CMD ["python", "-m", "poetry", "run", "python", "-m", "$SCRIPT"]
