FROM alpine:3.5

RUN mkdir /ta2-server

WORKDIR /ta2-server

COPY ./deploy/ta2-server .

EXPOSE 9500

ENTRYPOINT ./ta2-server
