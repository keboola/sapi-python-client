FROM python:3.11

WORKDIR /code
COPY . /code/
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir ".[dev]"
ENTRYPOINT ["python"]
