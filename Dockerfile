FROM python:3.9

WORKDIR /code

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ADD routes.py . 

EXPOSE 8000

CMD ["python", "./routes.py"]