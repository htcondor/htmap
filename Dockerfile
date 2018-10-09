FROM centos:7

# update and install basics

RUN yum update -y
RUN yum install -y git which wget sudo

# install htcondor

RUN wget https://research.cs.wisc.edu/htcondor/yum/RPM-GPG-KEY-HTCondor \
    && rpm --import RPM-GPG-KEY-HTCondor \
    && cd /etc/yum.repos.d \
    && wget https://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-stable-rhel7.repo \
    && yum install -y condor-all

COPY condor_config.docker_image /etc/condor/config.d/

# install global python 3

RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y install python36u
RUN yum -y install python36u-pip
RUN pip3.6 install cloudpickle
RUN ln -s /bin/python3.6 /usr/bin/python3

# create a user to run tests as

ENV SUBMIT_USER submitter
ENV PASS 123456
RUN useradd ${SUBMIT_USER} \
    && echo ${PASS} | passwd --stdin ${SUBMIT_USER} \
    && usermod -aG wheel ${SUBMIT_USER}
USER ${SUBMIT_USER}

# install miniconda

WORKDIR /home/${SUBMIT_USER}

RUN wget -q https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && bash Miniconda3-latest-Linux-x86_64.sh -p /home/${SUBMIT_USER}/miniconda -b \
    && rm Miniconda3-latest-Linux-x86_64.sh
ENV PATH=/home/${SUBMIT_USER}/miniconda/bin:${PATH}

# install htmap in virtual env

RUN conda create -n htmap python=3.6 -y

RUN mkdir /home/${SUBMIT_USER}/htmap
WORKDIR /home/${SUBMIT_USER}/htmap
COPY . .
RUN source activate htmap && pip install --no-cache-dir -r requirements_dev.txt && pip install .

# start condor and run htmap's tests

ENTRYPOINT echo ${PASS} | sudo -S /usr/sbin/condor_master \
    && sleep 5 \
    && condor_status \
    && source activate htmap \
    && pytest -n 10
