FROM python:3.11-buster as py-build

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
     && apt-get -y install --no-install-recommends netcat util-linux \
        vim bash-completion yamllint postgresql-client


RUN useradd -ms /bin/bash server
RUN chown -R server /home/server/

RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry python3 -
ENV PATH=/opt/poetry/bin:$PATH

WORKDIR /home/server/
USER server
COPY pyproject.toml ./
COPY poetry.lock ./
RUN mkdir seeds
RUN mkdir src
RUN mkdir src/dbt_remote
RUN mkdir src/dbt_server
RUN touch src/dbt_remote/__init__.py
RUN touch src/dbt_server/__init__.py
RUN touch README.md

RUN poetry config virtualenvs.in-project true
RUN poetry install --no-directory --only main
ADD src/ src/
RUN poetry install --no-interaction --only main

FROM python:3.11-slim-buster

RUN useradd -ms /bin/bash server
RUN chown -R server /home/server/

COPY --from=py-build /home/server/ /home/server/

ENV SCRIPT dbt-start-server
ENV LOGGING_SERVICE Local
ENV STORAGE_SERVICE LocalStorage
ENV METADATA_DOCUMENT_SERVICE Local
ENV JOB_SERVICE LocalJob

USER server
WORKDIR /home/server

CMD .venv/bin/$SCRIPT
