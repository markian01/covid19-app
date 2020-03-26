FROM python:3.8.1-slim-buster

ENV PYTHONDONTWRITEBYTECODE 1
ENV FLASK_ENV=production
ENV FLASK_APP=/app/app.py
RUN addgroup --system app && adduser --system --group app

WORKDIR /app
ADD ./ ./
RUN chown -R app:app .
RUN pip install -r requirements.txt

EXPOSE 5000
USER app

CMD gunicorn --bind 0.0.0.0:5000 manage:app
