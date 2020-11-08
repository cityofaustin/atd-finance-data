FROM atddocker/atd-oracle-py:production

# Copy our own application
WORKDIR /app
COPY . /app

RUN chmod -R 777 /app

# # Proceed to install the requirements...do
RUN cd /app && apt-get update && \
    pip install -r requirements.txt