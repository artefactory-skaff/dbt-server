FROM python:3.10

ENV SCRIPT dbt_server/server.py

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -ms /bin/bash newuser
WORKDIR /home/newuser/
ENV PYTHONPATH=/home/newuser
COPY . /home/newuser/dbt_server
RUN mkdir /home/newuser/.dbt

RUN chown -R newuser /home/newuser/
USER newuser

RUN mkdir seeds
CMD ["/bin/bash", "-c", "python3 $SCRIPT"]
