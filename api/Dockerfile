FROM python:3.10

# we define a work directory
WORKDIR /app

# we copy the code dir into the work directory
COPY requirements.txt /app

# we install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# we copy the code dir into the work directory
COPY job.py utils.py cloud_storage.py firestore.py /app/

# we execute the code with the following command
CMD [ "python3", "job.py" ]
