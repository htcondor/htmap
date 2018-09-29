FROM python:3.6.6

# install htcondor
RUN echo "deb http://research.cs.wisc.edu/htcondor/ubuntu/stable/ xenial contrib" >> /etc/apt/sources.list
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install condor -y

# install htmap
WORKDIR /home/htmap
COPY . .
RUN pip install --no-cache-dir -r requirements_dev.txt
RUN pip install .
