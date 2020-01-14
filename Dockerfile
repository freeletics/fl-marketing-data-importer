FROM python:3

ADD *.py /

ADD requirements.txt /

RUN pip3 install -r requirements.txt

CMD [ "python", "./markting_data_importer.py" ]