FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /Brazillian_Orders_Peter

#install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


COPY entrypoint.sh /Brazillian_Orders_Peter/entrypoint.sh
RUN chmod +x /Brazillian_Orders_Peter/entrypoint.sh

#copy the entire src directory, which includes the main files
COPY src /Brazillian_Orders_Peter/src
#copy the entire tests directory, which includes the test files
COPY tests /Brazillian_Orders_Peter/tests

COPY run_tests.py /Brazillian_Orders_Peter/run_tests.py















