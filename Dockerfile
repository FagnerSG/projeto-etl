FROM python:3

WORKDIR /src

ADD ./src /main

COPY ./requirements.txt /src/

RUN pip install -r /src/requirements.txt

CMD [ "python", "./main.py" ]