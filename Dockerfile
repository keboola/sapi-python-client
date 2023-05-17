FROM python:3.11

WORKDIR /code
COPY . /code/
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install --no-cache-dir flake8 responses typing_extensions
RUN python setup.py install
ENTRYPOINT ["python"]
