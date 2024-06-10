FROM python:3.10

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -ms /bin/bash dbt_user
RUN chown -R dbt_user /home/dbt_user/
WORKDIR /home/dbt_user/
USER dbt_user

COPY . ./server
RUN mkdir .dbt


ENV PORT=8080
ENV PYTHONPATH=.

CMD ["/bin/bash", "-c", "python3 server/main.py"]
