FROM python:3.6-alpine

RUN apk add --no-cache \
    perl \
    pgbadger

WORKDIR /usr/src/app

COPY Pipfile .
COPY Pipfile.lock .
RUN pip install --no-cache-dir pipenv && pipenv install --system

COPY . .

CMD [ "python", "-m", "rds_pgbadger" ]
