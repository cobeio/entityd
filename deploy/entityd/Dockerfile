FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install python3-pip libffi-dev libyaml-dev libzmq3-dev \
    && apt-get -y clean
LABEL entityd=0.20.0 \
    image=v1
RUN apt-get -y install mercurial \
    && pip3 install hg+https://bitbucket.org/cobeio/syskit@6c00612b3d80f1d7c9fad41a2f1a24992a416881#egg=syskit \
    && apt-get -y remove mercurial \
    && apt-get -y autoremove --purge \
    && apt-get -y clean
RUN pip3 install entityd
COPY wrap.sh /usr/local/bin/wrap.sh
ENTRYPOINT ["/usr/local/bin/wrap.sh"]
CMD ["--help"]
