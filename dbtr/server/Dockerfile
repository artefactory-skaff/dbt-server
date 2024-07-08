FROM python:3.10

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -ms /bin/bash dbt_user
RUN chown -R dbt_user /home/dbt_user/
RUN chown -R dbt_user /usr/local/lib/python3.10/
WORKDIR /home/dbt_user/

COPY dist/server ./dbtr/server
COPY dist/common ./dbtr/common
RUN mkdir .dbt

ENV PORT=8080
ENV PYTHONPATH=.

RUN chmod +x dbtr/server/startup.sh

USER dbt_user
CMD ["/bin/bash", "-c", "dbtr/server/startup.sh"]
