FROM ubuntu:20.04

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get install -y git cmake wget python3 python3-pip
RUN python3 -m pip install -U pip

# python requirements
ADD requirements.txt /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements.txt

# app
COPY . /app
WORKDIR /app

ENV PYTHONPATH "${PYTHONPATH}:/app"

# entrypoint
ENTRYPOINT ["python3", "ledger.py"]
