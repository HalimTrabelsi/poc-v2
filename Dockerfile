
FROM bitnami/python:3.10.13-debian-11-r24

ARG container_user=openg2p
ARG container_user_group=openg2p
ARG container_user_uid=1001
ARG container_user_gid=1001

RUN groupadd -g ${container_user_gid} ${container_user_group} \
  && useradd -mN -u ${container_user_uid} -G ${container_user_group} -s /bin/bash ${container_user}

WORKDIR /app

RUN install_packages libpq-dev \
  && apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

COPY --chown=${container_user}:${container_user_group} . /app/src
COPY --chown=${container_user}:${container_user_group} main.py /app

RUN python3 -m pip install --no-cache-dir \
  git+https://github.com/openg2p/openg2p-fastapi-common@v1.1.2#subdirectory=openg2p-fastapi-common \
  git+https://github.com/openg2p/openg2p-fastapi-common@v1.1.2#subdirectory=openg2p-fastapi-auth \
  git+https://github.com/openg2p/openg2p-g2pconnect-common-lib@v1.1.0#subdirectory=openg2p-g2pconnect-common-lib \
  git+https://github.com/openg2p/openg2p-g2pconnect-common-lib@v1.1.0#subdirectory=openg2p-g2pconnect-mapper-lib \
  ./src

USER ${container_user}

ENV SPAR_MAPPER_WORKER_TYPE=local \
    SPAR_MAPPER_HOST=0.0.0.0 \
    SPAR_MAPPER_PORT=8000 \
    SPAR_MAPPER_NO_OF_WORKERS=2

CMD ["/bin/bash","-lc","python3 main.py migrate && exec gunicorn 'main:app' --workers ${SPAR_MAPPER_NO_OF_WORKERS} --worker-class uvicorn.workers.UvicornWorker --bind ${SPAR_MAPPER_HOST}:${SPAR_MAPPER_PORT}"]
