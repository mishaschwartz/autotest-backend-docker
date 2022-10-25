# autotest-backend-docker

This repo contains a backend for the Autotester project that runs in Docker.

Tests are run within docker containers in the `/`



## System requirements

- [Docker Engine or Docker Desktop](https://docs.docker.com/)
- [python3.8+](https://www.python.org/)
- [redis-server](https://redis.io/)

## Installation

Install the required python packages using pip

```sh
python3 -m pip install -r requirements.txt
```

## Configuration

The backend can be configured by starting up the worker containers with the following environment variables set:

- REDIS_URL
  - Url of the redis database. This should be the same url set for the autotest API or else the two cannot communicate
- REGISTRY_URL
  - optional URL of a docker registry used to store images created by this backend. If not set, images will be stored locally only.
  - see the [registry documentation](https://docs.docker.com/registry/) for more details
- VOLUME_DRIVER
  - optional volume driver type used when creating volumes. Default=local.
  - see the [volume documentation](https://docs.docker.com/storage/volumes/) for more details
- VOLUME_DRIVER_OPTS
  - optional json string containing volume driver options.
  - see the [driver_opts](https://docker-py.readthedocs.io/en/stable/volumes.html#docker.models.volumes.VolumeCollection.create) example for more details on how to structure the json string.
  - see the [bind mount documentation](https://docs.docker.com/storage/bind-mounts/) for more details

These environment variable values can be set in the `services.backend-*.environment` list in `docker-compose.yml` 

The number of worker containers can be adjusted by changing the `replicas` value in `docker-compose.yml`. The larger the
`replicas` value, the more tests can be run simultaneously. Make sure to not set this value so high that it overtaxes
the resources of your machine.

## Start up

### In development

```shell
docker compose up -d backend-dev
```

### In production

```shell
docker compose up -d backend-prod
```

## Security Note

This backend requires that the docker socket file be mounted to the worker containers so that they can access the docker
engine to create other docker artifacts used to run tests. 

If you search the internet you will find many websites discussing why mounting the docker socket to a container is a 
security risk. In the context of this project, it allows the worker containers to run processes as root on the host machine. 

If this is not something you are comfortable with, we recommend running docker in [rootless mode](https://docs.docker.com/engine/security/rootless/)
in order to mitigate some of the risks involved. 
