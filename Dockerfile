FROM python:3.9

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY src /code/src
COPY ./main.py /code/main.py
EXPOSE 8000

ENV PYTHONPATH "${PYTHONPATH}:/code/src"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]
