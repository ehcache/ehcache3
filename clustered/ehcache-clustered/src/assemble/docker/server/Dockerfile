FROM azul/zulu-openjdk-alpine:17.0.5-17.38.21

RUN apk update && apk upgrade && apk add bash
RUN mkdir /terracotta && chgrp -R 0 /terracotta && chmod -R g+rX /terracotta
RUN mkdir /terracotta/config && chgrp -R 0 /terracotta/config && chmod -R g+rX /terracotta/config
RUN mkdir /terracotta/run && chgrp -R 0 /terracotta/run && chmod -R g+rwX /terracotta/run

WORKDIR /terracotta

# This volume can be mounted with "ro" flag.
# This is where the configurations will be sourced and eventually copied elsewhere within the container.
VOLUME "/terracotta/config"

# This folder can be mounted with "rw" flag.
# This is where the default configurations will output any logs, data, etc
# If the volume is mounted, the container will support being restarted in the same state.
# If a volume is mounted, be sure to set the correct chmod permissions in the host machine.
# Also, you might need to use a special cleanup task to remove the created files in the host machine
VOLUME "/terracotta/run"

EXPOSE 9410

ENV DEFAULT_OFFHEAP "offheap-1:512MB,offheap-2:512MB"
ENV DEFAULT_ACTIVATE "false"
ENV DEFAULT_CLUSTER_NAME "tc-cluster"
ENV DEFAULT_FAILOVER "availability"

COPY server /terracotta/server
COPY docker/server/entrypoint.sh /terracotta/server/entrypoint.sh

ENTRYPOINT ["./server/entrypoint.sh"]
