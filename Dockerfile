FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /Brazillian_Orders_Peter

#install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
# --no-cache-dir 
#copy the entrypoint.sh file to the container
COPY entrypoint.sh /Brazillian_Orders_Peter/entrypoint.sh
RUN chmod +x /Brazillian_Orders_Peter/entrypoint.sh

#copy the data directory to the container
COPY data /Brazillian_Orders_Peter/data

EXPOSE 8080
#copy the entire src directory, which includes the main files
COPY src /Brazillian_Orders_Peter/src
#copy the entire tests directory, which includes the test files
COPY tests /Brazillian_Orders_Peter/tests
#copy the run_tests.py file to the container
COPY run_tests.py /Brazillian_Orders_Peter/run_tests.py















