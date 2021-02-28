FROM node:14 as consolebuilder
WORKDIR /app
ARG CONSOLE_VERSION=3.4.12
RUN npm install -g npm@7
RUN git clone --depth 1 --branch v${CONSOLE_VERSION} https://github.com/janelia-flyem/dvid-console.git .
RUN npm install
RUN npm run build

FROM ubuntu:20.04 as builder
ARG DVID_VERSION=0.9.12
ARG CONSOLE_VERSION=3.4.12
MAINTAINER flyem project team
LABEL maintainer="neuprint@janelia.hhmi.org"
LABEL dvid_version=${DVID_VERSION}
LABEL console_version=${CONSOLE_VERSION}
RUN apt-get update && apt-get install -y curl bzip2
WORKDIR /app/
COPY --from=consolebuilder /app/build /console
RUN curl -L -O https://github.com/janelia-flyem/dvid/releases/download/v${DVID_VERSION}/dvid-${DVID_VERSION}-dist-linux.tar.bz2
RUN tar -jxf dvid-${DVID_VERSION}-dist-linux.tar.bz2
RUN ln -s /app/dvid-${DVID_VERSION}-dist-linux/bin/dvid /usr/local/bin
COPY ./conf/config.example /conf/config.toml
CMD ["dvid", "-verbose", "serve", "/conf/config.toml"]
#CMD ["/bin/bash"]
