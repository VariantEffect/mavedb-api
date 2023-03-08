FROM python:3.9

# Generate a self-signed certificate. This Docker image is for use behind a load balancer or other reverse proxy, so it
# can be self-signed and does not need a real domain name.
RUN mkdir -p /code/ssl
RUN openssl req -nodes -x509 \
    -newkey rsa:4096 \
    -sha256 \
    -keyout /code/ssl/server.key \
    -out /code/ssl/server.cert \
    -subj "/C=US/ST=Washington/L=Seattle/O=University of Washington/OU=Brotman Baty Institute/CN=mavedb-api"

WORKDIR /code

# Install Python packages.
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Install the application code.
COPY alembic /code/alembic
COPY alembic.ini /code/alembic.ini
COPY src /code/src
COPY ./main.py /code/main.py

# Tell Docker that we will listen on port 3000.
EXPOSE 3000

# Set up the path Python will use to find modules.
ENV PYTHONPATH "${PYTHONPATH}:/code/src"

# At container startup, run the application using uvicorn.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]
