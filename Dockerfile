FROM python:3.6

# install htcondor
RUN echo "deb http://htcondor.org/debian/stable/ jessie contrib  " >> /etc/apt/sources.list
RUN wget -qO - http://research.cs.wisc.edu/htcondor/debian/HTCondor-Release.gpg.key | apt-key add -
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install condor -y

# install htmap
WORKDIR /home/htmap
COPY . .
RUN pip install --no-cache-dir -r requirements_dev.txt
RUN pip install .
