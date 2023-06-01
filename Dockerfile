FROM python:3.9

WORKDIR /usr/src/app

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python", "-u", "crawler.py"]