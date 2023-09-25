FROM python:3.10-slim

ENV SCRIPT dbt_server.py
ENV LOGGING_SERVICE Local
ENV STORAGE_SERVICE LocalStorage
ENV METADATA_DOCUMENT_SERVICE Local
ENV JOB_SERVICE LocalJob

RUN useradd -ms /bin/bash local
RUN chown -R local /home/local/

WORKDIR /home/local/
USER local

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

RUN python -m poetry install --no-interaction --only main

ADD src/ src/

CMD ["python", "-m", "poetry", "run", "python", "-m", "$SCRIPT"]
