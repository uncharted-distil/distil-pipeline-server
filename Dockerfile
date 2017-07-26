FROM alpine:3.5

RUN mkdir /distil-pipeline-server

WORKDIR /distil-pipeline-server

COPY ./deploy/distil-pipeline-server .

EXPOSE 9500

ENTRYPOINT ./distil-pipeline-server
