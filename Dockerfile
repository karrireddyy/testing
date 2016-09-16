FROM ubuntu:14.04

RUN apt-get update --fix-missing

RUN apt-get install -y build-essential git
RUN apt-get install -y python python-dev python-setuptools
RUN apt-get install -y python-pip python-virtualenv
RUN apt-get install -y vim htop gettext
RUN apt-get install -y wget libffi-dev libssl-dev

RUN pip install --upgrade pip

RUN mkdir /ocropus_install
RUN git init .
RUN git pull https://github.com/tmbdev/ocropy
RUN apt-get install -y $(cat PACKAGES)
RUN wget -nd http://www.tmbdev.net/en-default.pyrnn.gz
RUN mv en-default.pyrnn.gz models/
RUN python setup.py install
#following is for HTML output for training
RUN apt-get install -y libxml2-dev libxslt-dev python-dev lib32z1-dev
RUN pip install lxml
#End note

RUN apt-get install -y ghostscript
RUN apt-get install -y python-poppler poppler-utils
RUN apt-get install -y imagemagick

RUN apt-get install -y groff
RUN pip install awscli

RUN apt-get install -y libcap-dev
RUN apt-get install -y libffi-dev libssl-dev

WORKDIR /app
ADD ./requirements.txt /app/worker/requirements.txt
RUN pip install -r worker/requirements.txt
ADD . /app

RUN mkdir /root/.ssh/
ADD id_rsa /root/.ssh/id_rsa
RUN chmod 400 /root/.ssh/id_rsa
RUN chown -R root:root /root/.ssh
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN git clone git@github.com:Cedarwood-Consulting/bookreport-common.git common-repo
RUN rm -rf common
RUN mv common-repo/src common
RUN rm -rf common-repo
# RUN pip install -r common/requirements.txt
RUN pip install pypdf
RUN sudo apt-get install -y python-pythonmagick
RUN pip install reportlab
RUN sudo apt-get install -y tesseract-ocr
RUN pip install pytesseract
RUN pip install tesseract
RUN pip install https://github.com/pika/pika/archive/master.zip

CMD ["python","-u","content_extraction_pdf.py"]
