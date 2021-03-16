FROM python:3.8

WORKDIR /code
COPY . /code/
RUN pip3 install --no-cache-dir flake8 responses
RUN python setup.py install
ENTRYPOINT ["python"]
