#!/bin/sh

# Last week I didn't use a named volume as I was torching the database every time
#docker run -d --name postgres -p 5432:5432 postgre
# This time, I'm using a named volume so that the data will persist between containers
# In order to do this, use the following command to see the current volumes::
# docker volume ls
# Then, to remove an existing volume:
# docker volume rm volName
# And to add a named volume:
# docker volume create volName
# And then, to see what's happened:
# docker volume ls
# More information available here : [ https://docs.docker.com/engine/reference/commandline/volume_create/ ]
# And here : [ https://linuxize.com/post/how-to-remove-docker-images-containers-volumes-and-networks/ ]
docker run -d -v postgresData:/var/lib/postgresql/data --name postgres -p 5432:5432 postgres
