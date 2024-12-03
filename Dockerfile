FROM python:3.12-slim

WORKDIR /app

# Download, install, and configure the CS package repo.
RUN apt-get update && apt-get install -y curl
RUN curl -LsSO https://r.mariadb.com/downloads/mariadb_repo_setup
RUN echo "6083ef1974d11f49d42ae668fb9d513f7dc2c6276ffa47caed488c4b47268593  mariadb_repo_setup" | sha256sum -c -
RUN ./mariadb_repo_setup

# Install required packages for the mariadb library using the CS package repo.
RUN apt-get install -y libmariadb3 libmariadb-dev

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "src/customer_data_to_mariadb.py" ]