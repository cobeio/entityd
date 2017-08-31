FROM python:3.4-alpine3.4
RUN apk update
RUN apk add build-base
RUN apk add libffi-dev
RUN apk add yaml-dev
RUN apk add zeromq-dev
RUN apk add mercurial
RUN pip3 install virtualenv
RUN virtualenv /opt/entityd/ -p python3.4
COPY ./requirements.txt /entityd/requirements.txt
COPY ./test_requirements.txt /entityd/test_requirements.txt
RUN /opt/entityd/bin/pip3 install -r /entityd/test_requirements.txt
ARG VERSION=0.21.0
LABEL entityd=${VERSION} \
    image=v1
COPY ./ /entityd
WORKDIR /entityd
RUN /opt/entityd/bin/pip install -e .

# Final image; only test-time dependencies.
FROM python:3.4-alpine3.4
COPY --from=0 /entityd/ /entityd/
COPY --from=0 /opt/entityd/ /opt/entityd/
RUN apk add --no-cache bash
RUN apk add --no-cache libzmq
## The tests assume the user doesn't have root privledges so we run
## as a standard user
RUN adduser -D -s /bin/bash user \
    && chown -R user:user /entityd \
    && chown -R user:user /opt/entityd
USER user
WORKDIR /entityd
ENV PATH $PATH:/opt/entityd/bin
ENTRYPOINT ["/opt/entityd/bin/invoke"]
CMD ["--help"]
