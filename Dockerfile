FROM python:3.9-slim-buster

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/usr/app/src/"

# Initializing new working directory
WORKDIR /usr/app/src 

# Transferring the code and essential data
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY run.py ./
COPY S3BucketConnector.py ./
COPY TandemDataTransformer.py ./
COPY TandemScraper.py ./
COPY report_config.py ./configs/

RUN pip install pipenv
RUN pipenv install --ignore-pipfile --system