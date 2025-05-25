FROM python:3.9
WORKDIR /app
RUN pip install fastapi
RUN pip install uvicorn
RUN pip install pandas
COPY . /app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port",  "8000"]