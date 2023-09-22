FROM python:3.10-slim

RUN useradd -ms /bin/bash package
RUN chown -R package /home/package/

WORKDIR /home/package/
USER package

RUN pip install poetry

RUN mkdir package/
RUN mkdir api/
RUN touch api/__init__.py
COPY pyproject.toml ./
COPY poetry.lock ./

RUN python -m poetry install --no-interaction --only main

ADD package/ package/

ENTRYPOINT ["python", "-m", "poetry", "run", "dbt-remote"]
