FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install python3-pip libffi-dev libyaml-dev libzmq3-dev \
    && apt-get -y clean
LABEL entityd=0.18.1 \
    image=v1
RUN pip3 install entityd
COPY wrap.sh /usr/local/bin/wrap.sh
ENTRYPOINT ["/usr/local/bin/wrap.sh"]
CMD ["--help"]
