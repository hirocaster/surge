FROM hirocaster/docker-elixir

RUN mkdir /root/workspace

WORKDIR /root/workspace
COPY . /root/workspace

RUN apk add python python-dev py-pip build-base
RUN pip install docker-compose

ENTRYPOINT [ \
  "prehook", \
    "elixir -v", \
    "docker-compose --version", \
    "mix deps.get", "--", \
  "switch", \
    "shell=/bin/sh", "--", \
  "codep", \
    "mix test" \
]
