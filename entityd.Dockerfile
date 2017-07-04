FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install python3-pip libffi-dev libyaml-dev libzmq3-dev \
    && apt-get -y clean

RUN pip3 install virtualenv
RUN virtualenv /venvs/entityd -p python3.4

RUN apt-get -y install mercurial

## Work around docker always skipping the cache and running pip each build.
## This way it will only run pip install if the requirements change
COPY ./requirements.txt /entityd/requirements.txt
RUN /venvs/entityd/bin/pip3 install -r /entityd/requirements.txt

RUN apt-get -y remove mercurial \
    && apt-get -y autoremove --purge \
    && apt-get -y clean

ARG VERSION=0.21.0
LABEL entityd=${VERSION} \
    image=v1

COPY ./ /entityd
WORKDIR /entityd
RUN /venvs/entityd/bin/pip install -e .

COPY deploy/entityd/wrap.sh /usr/local/bin/wrap.sh
ENTRYPOINT ["/usr/local/bin/wrap.sh"]
CMD ["--help"]
