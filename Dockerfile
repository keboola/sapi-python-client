FROM python:3.6

WORKDIR /code
COPY . /code/
python setup.py install
ENTRYPOINT ["python"]
