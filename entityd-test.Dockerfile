FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install python3-pip libffi-dev libyaml-dev libzmq3-dev \
    && apt-get -y clean

# Install docker-ce so the tests can run
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && add-apt-repository \
           "deb [arch=amd64] https://download.docker.com/linux/debian \
           $(lsb_release -cs) \
           stable" \
    && apt-get -y update \
    && apt-get -y install docker-ce

RUN pip3 install virtualenv
RUN virtualenv /venvs/entityd -p python3.4

## Work around docker always skipping the cache and running pip each build.
## This way it will only run pip install if the requirements change
COPY ./requirements.txt /entityd/requirements.txt
COPY ./test_requirements.txt /entityd/test_requirements.txt
RUN apt-get -y install mercurial \
    && /venvs/entityd/bin/pip3 install -r /entityd/test_requirements.txt \
    && apt-get -y remove mercurial \
    && apt-get -y autoremove --purge \
    && apt-get -y clean

COPY /pylint-abilisoft /pylint_abilisoft
RUN /venvs/entityd/bin/pip3 install -e /pylint_abilisoft

ARG VERSION=0.21.0
LABEL entityd=${VERSION} \
    image=v1

COPY ./ /entityd
WORKDIR /entityd
RUN /venvs/entityd/bin/pip install -e .

COPY deploy/entityd/wrap.sh /usr/local/bin/wrap.sh

## The tests assume the user doesn't have root privledges so we run
## as a standard user
ARG DOCKER_GID=1001
RUN useradd --create-home -s /bin/bash user \
    && chown -R user:user /entityd \
    && chown -R user:user /venvs/entityd \
    && groupadd -g ${DOCKER_GID} docker_host \
    && usermod -a -G docker,docker_host user

USER user

ENTRYPOINT ["/venvs/entityd/bin/invoke"]
CMD ["--help"]
