FROM python:3.10

ENV SCRIPT dbt_server.py

# we copy Pythons' requirements to current dir and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# we create a new user
RUN useradd -ms /bin/bash newuser

WORKDIR /home/newuser/
COPY . /home/newuser/

RUN chown -R newuser /home/newuser/
# grant permission to elementary, needed to generate report
RUN chown -R newuser /usr/local/lib/python3.10/site-packages/elementary/ 
USER newuser

# we run relevant script
CMD ["/bin/bash", "-c", "if [[ \"$SCRIPT\" == \"dbt_run_job.py\" ]] ; then python3 dbt_run_job.py && python3 elementary_report.py ; else python3 $SCRIPT ; fi"]

