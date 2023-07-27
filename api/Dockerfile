FROM python:3.10

ENV SCRIPT dbt-server.py

# we copy the code dir into the work directory
COPY requirements.txt .

# we install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# we copy the code dir into the work directory

RUN useradd -ms /bin/bash newuser

USER newuser
WORKDIR /home/newuser
COPY . /home/newuser/


# we execute the code with the following command
CMD ["sh", "-c", "python3 $SCRIPT"]
