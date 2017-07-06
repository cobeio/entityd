FROM debian:8.5
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install wget \
    && apt-get -y clean
ENV KUBE_VERSION v1.3.0
ENV KUBE_SRC_URL https://github.com/kubernetes/kubernetes/releases/download/${KUBE_VERSION}/kubernetes.tar.gz
RUN mkdir -p /usr/local/src \
    && wget -qO /usr/local/src/kubernetes.tar.gz $KUBE_SRC_URL \
    && cd /usr/local/src \
    && tar -xf kubernetes.tar.gz \
    && cd - \
    && cp /usr/local/src/kubernetes/platforms/linux/amd64/kubectl /usr/local/bin/ \
    && rm -rf /usr/local/src/
EXPOSE 8001
ENTRYPOINT ["/usr/local/bin/kubectl"]
CMD ["proxy", "--port=8001"]
