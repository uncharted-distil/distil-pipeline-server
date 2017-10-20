# distil-pipeline-server

Provides a stub server for testing Distil GRPC calls.

[![CircleCI](https://circleci.com/gh/unchartedsoftware/distil-pipeline-server/tree/master.svg?style=svg&circle-token=6037bacad70b27a10c6fb2d21d31ed75fc6734ab)](https://circleci.com/gh/unchartedsoftware/distil-pipeline-server/tree/master)

## Dependencies

- [Go](https://golang.org/) programming language binaries with the `GOPATH` environment variable specified and `$GOPATH/bin` in your `PATH`.

## Development

Clone the repository:

```bash
mkdir -p $GOPATH/src/github.com/unchartedsoftware
cd $GOPATH/src/github.com/unchartedsoftware
git clone git@github.com:unchartedsoftware/distil-pipeline-server.git
```

Install dependencies:

```bash
cd distil-pipeline-server
make install
```

Build, watch, and run server:
```bash
make watch
```


## Generating GRPC/Protobuf Source

If changes are made to the `*.proto` files that requires source to be re-generated, install protocol buffer compiler:

Linux

```bash
curl -OL https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-linux-x86_64.zip
unzip protoc-3.3.0-linux-x86_64.zip -d protoc3
sudo cp protoc3/bin/protoc /usr/bin/protoc
sudo cp -r protoc3/include /usr/local
```

OSX

```bash
curl -OL https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-osx-x86_64.zip
unzip protoc-3.3.0-osx-x86_64.zip -d protoc3
sudo cp protoc3/bin/protoc /usr/bin/protoc
sudo cp -r protoc3/include /usr/local
```

Generate GRPC source from proto definition:
```base
make proto
```

## Docker Deployment

Create a docker image from current source:
```bash
cd deploy
./docker_build.sh
```

Run the container:
```bash
./docker_run.sh
```

## Releasing

1.  Tag source using semantic versioning.
2.  Update the tags in `docker_build.sh` and `docker_push.sh` to match the source tag.
3.  Build and push.

## Common Issues:

#### "glide: command not found":

- **Cause**: `$GOPATH/bin` has not been added to your `$PATH`.
- **Solution**: Add `export PATH=$PATH:$GOPATH/bin` to your `.bash_profile` or `.bashrc`.

#### "../repo/subpackage/file.go:10:2: cannot find package "github.com/company/package/subpackage" in any of":

- **Cause**: Dependencies are out of date or have not been installed
- **Solution**: Run `make install` to install latest dependencies.
