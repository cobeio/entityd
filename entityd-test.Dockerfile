FROM eu.gcr.io/cobesaas/python:3.4-alpine-pypi as build
RUN apk update
RUN apk add build-base
RUN apk add libffi-dev
RUN apk add yaml-dev
RUN apk add zeromq-dev
RUN apk add mercurial
RUN pip3 install virtualenv

RUN virtualenv /opt/cobe-agent/ -p python3.4
## Work around docker always skipping the cache and running pip each build.
## This way it will only run pip install if the requirements change

COPY ./requirements.txt /opt/cobe-agent/entityd/src/requirements.txt
RUN /opt/cobe-agent/bin/pip3 install \
    -r /opt/cobe-agent/entityd/src/requirements.txt

COPY ./test_requirements.txt /opt/cobe-agent/entityd/src/test_requirements.txt
RUN /opt/cobe-agent/bin/pip3 install \
    -r /opt/cobe-agent/entityd/src/test_requirements.txt

ARG VERSION=0.21.0
LABEL entityd=${VERSION} \
    image=v1
COPY ./ /opt/cobe-agent/src/
WORKDIR /opt/cobe-agent/
RUN /opt/cobe-agent/bin/pip3 install -e src/


# Final image; only test-time dependencies.
FROM python:3.4-alpine3.4
RUN apk add --no-cache libzmq
RUN apk add --no-cache curl
RUN apk add --no-cache bash

## The tests assume the user doesn't have root privledges so we run
## as a standard user
RUN adduser -D -s /bin/sh user
USER user

COPY --from=build --chown=user:user /opt/cobe-agent/ /opt/cobe-agent/
ENV PATH $PATH:/opt/cobe-agent/bin
WORKDIR /opt/cobe-agent/src/
ENTRYPOINT ["/opt/cobe-agent/bin/invoke"]
CMD ["--help"]
