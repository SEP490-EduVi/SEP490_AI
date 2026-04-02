FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN mkdir -p /app/credentials

COPY game_quiz/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY game_quiz/ ./game_quiz/

CMD ["python", "-m", "game_quiz.main"]
