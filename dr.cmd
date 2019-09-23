@echo off

SET CONTAINER_TAG=htmap-tests

ECHO Building HTMap testing container...

docker build --quiet -t %CONTAINER_TAG% --file tests/_inf/Dockerfile .
docker run -it --rm %CONTAINER_TAG% %*
