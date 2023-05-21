FROM python:3.11

WORKDIR /code
COPY . /code/
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir flake8
RUN pip install --no-cache-dir .
ENTRYPOINT ["python"]
