FROM debian

RUN apt-get update -y && apt-get upgrade -y

RUN apt-get install -y \
    openjdk-8-jdk \
    python-setuptools \
    python-pip \
    virtualenv

#ENTRYPOINT ['/bin/bash']