FROM python:3.10-slim as base

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt && rm requirements.txt

FROM base as dev
# same as base

ENTRYPOINT ["/app/.dockerfiles/entrypoint-dev.sh"]

CMD ["rq", "worker", "-c", "rq_settings"]

FROM base as prod

COPY . .

ENTRYPOINT ["/app/.dockerfiles/entrypoint-prod.sh"]

CMD ["rq", "worker", "-c", "rq_settings"]
