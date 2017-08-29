FROM python:3.4-alpine3.4
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

ARG VERSION=0.21.0
LABEL entityd=${VERSION} \
    image=v1

COPY ./ /opt/cobe-agent/src/
WORKDIR /opt/cobe-agent/
RUN /opt/cobe-agent/bin/pip3 install -e src/

ENV PATH $PATH:/opt/cobe-agent/bin
RUN apk add curl
# RUN apk add uuid
RUN mkdir /opt/entityd/
RUN ln -s /opt/cobe-agent/src/cobe-agent.sh /opt/cobe-agent/bin/
ENTRYPOINT ["/opt/cobe-agent/bin/cobe-agent.sh"]
CMD ["--help"]
