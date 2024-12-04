FROM python:3.12

WORKDIR /app

# Install required packages for the mariadb Python SDK.
RUN apt-get update && apt-get install -y libmariadb3 libmariadb-dev

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "src/customer_data_to_mariadb.py" ]