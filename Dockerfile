FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /Brazillian_Orders_Peter

#install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

#copy the entire src directory, which includes the main files
COPY src /Brazillian_Orders_Peter/src
COPY data /Brazillian_Orders_Peter/data

#copy the entire tests directory, which includes the test files
COPY tests /Brazillian_Orders_Peter/tests

# Set the working directory to where the test files are located
WORKDIR /Brazillian_Orders_Peter/tests
RUN python -m unittest -v

WORKDIR /Brazillian_Orders_Peter















