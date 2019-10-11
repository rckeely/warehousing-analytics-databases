#!/bin/sh

docker run --rm -it --net host postgres psql --host 0.0.0.0 --user postgres
