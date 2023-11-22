FROM python:3.9.5-alpine3.13

RUN apk update && apk upgrade && apk add ca-certificates

RUN mkdir /app/
ADD . /app/
WORKDIR /app

RUN pip install pipenv
RUN pipenv install --system --deploy

ENTRYPOINT [ "python", "./src/main.py" ]
CMD []