FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install python3-pip libffi-dev libyaml-dev libzmq3-dev \
    && apt-get -y clean
RUN pip3 install entityd
LABEL entityd=0.11.0 \
      image=v1
COPY wrap.sh /usr/local/bin/wrap.sh
ENTRYPOINT ["/usr/local/bin/wrap.sh"]
CMD ["--help"]
