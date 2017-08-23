FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y install python3 python3-pip \
    && apt-get -y autoremove --purge

RUN pip3 install virtualenv
RUN virtualenv /opt/entityd -p python3.4

## Work around docker always skipping the cache and running pip each build.
## This way it will only run pip install if the requirements change
COPY ./requirements.txt /entityd/requirements.txt

RUN apt-get -y install mercurial \
    && /opt/entityd/bin/pip3 install -r /entityd/requirements.txt \
    && apt-get -y remove mercurial \
    && apt-get -y autoremove --purge

ARG VERSION=0.21.0
LABEL entityd=${VERSION} \
    image=v1

COPY ./ /entityd
WORKDIR /entityd
RUN /opt/entityd/bin/pip3 install -e .

ENV PATH $PATH:/opt/entityd/bin
RUN apt-get install -y curl
RUN apt-get install -y uuid
COPY deploy/entityd/wrap.sh /usr/local/bin/wrap.sh
ENTRYPOINT ["/usr/local/bin/wrap.sh"]
CMD ["--help"]
