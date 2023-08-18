FROM python:3.10

ENV SCRIPT dbt_server.py

# we copy the code dir into the work directory
COPY requirements.txt .

# we install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["/bin/bash", "-c", "if [[ \"$SCRIPT\" == \"job.py\" ]] ; then python3 job.py && python3 elementary_report.py ; else python3 $SCRIPT ; fi"]


# RUN useradd -ms /bin/bash newuser

# WORKDIR /home/newuser
# COPY . /home/newuser/

# RUN chown -R newuser /home/newuser
# USER newuser

# CMD ["/bin/bash", "-c", "if [[ \"$SCRIPT\" == \"job.py\" ]] ; then python3 job.py && python3 elementary_report.py ; else python3 $SCRIPT ; fi"]
