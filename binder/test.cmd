@echo off

SET CONTAINER_TAG=htmap-binder-test

ECHO Building HTMap Binder container...

docker build -t %CONTAINER_TAG% --file binder/Dockerfile .
docker run -it --rm -p 8888:8888 %CONTAINER_TAG% %*
