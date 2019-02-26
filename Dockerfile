FROM cyclus/cycamore

COPY ./requirements.txt /tmp
RUN pip install -U pip \
    && pip install -r /tmp/requirements.txt

COPY . /code
WORKDIR /code

CMD python -m recyclus_worker
