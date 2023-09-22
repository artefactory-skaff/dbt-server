FROM python:3.10-slim

ENV SCRIPT dbt_server.py
ENV LOGGING_SERVICE Local
ENV CLOUD_STORAGE_SERVICE LocalStorage
ENV METADATA_DOCUMENT_SERVICE Local
ENV JOB_SERVICE LocalJob

RUN useradd -ms /bin/bash local
RUN chown -R local /home/local/

WORKDIR /home/local/
USER local

RUN pip install poetry

RUN mkdir package/
RUN touch package/__init__.py
RUN touch package/README.md
RUN mkdir api/
RUN touch api/__init__.py
COPY pyproject.toml ./
COPY poetry.lock ./
RUN mkdir seeds

RUN python -m poetry install --no-interaction --only main

ADD api/ api/

CMD ["python", "-m", "poetry", "run", "python", "-m", "$SCRIPT"]
