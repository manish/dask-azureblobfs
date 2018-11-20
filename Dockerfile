FROM python:3.6 AS base
ARG CI_USER_TOKEN
ARG AZURE_BLOB_ACCOUNT_NAME
ARG AZURE_BLOB_ACCOUNT_KEY
RUN echo "machine github.com\n  login $CI_USER_TOKEN\n" > ~/.netrc

ENV \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  PIPENV_HIDE_EMOJIS=true \
  PIPENV_COLORBLIND=true \
  PIPENV_NOSPIN=true \
  PYTHONPATH="/app:${PYTHONPATH}"
RUN pip3 install pipenv==2018.5.18 pip==18.0

WORKDIR /build
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv install --system --deploy --ignore-pipfile --dev

WORKDIR /app
