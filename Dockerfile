FROM python:3.9-slim
WORKDIR /Brazillian_Orders_Peter

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock
RUN pip install poetry
RUN poetry install

COPY entrypoint.sh /Brazillian_Orders_Peter/entrypoint.sh
RUN chmod +x /Brazillian_Orders_Peter/entrypoint.sh

COPY data /Brazillian_Orders_Peter/data


EXPOSE 8080

COPY src /Brazillian_Orders_Peter/src
COPY tests /Brazillian_Orders_Peter/tests
COPY run_tests.py /Brazillian_Orders_Peter/run_tests.py

ENTRYPOINT [ "/Brazillian_Orders_Peter/entrypoint.sh" ]
