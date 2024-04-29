FROM python:3.11-slim
LABEL authors="opa-oz"

WORKDIR /code

COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./main.py /code/main.py

VOLUME /code/config

CMD ["python3", "main.py"]