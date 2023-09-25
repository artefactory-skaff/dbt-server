FROM python:3.10-slim

RUN useradd -ms /bin/bash package
RUN chown -R package /home/package/

WORKDIR /home/package/
USER package

RUN pip install poetry

COPY pyproject.toml ./
COPY poetry.lock ./
RUN mkdir src
RUN mkdir src/dbt_remote
RUN mkdir src/dbt_server
RUN touch src/dbt_remote/__init__.py
RUN touch src/dbt_server/__init__.py
RUN touch README.md

RUN python -m poetry install --no-interaction --only main

ADD src/ src/

ENTRYPOINT ["python", "-m", "poetry", "run", "dbt-remote"]
