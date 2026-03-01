@echo off
REM ============================================================
REM  Redeploy lesson-analysis service after code changes
REM  Run from: D:\SEP490\SEP490_AI
REM ============================================================

REM 1. Stop the running lesson-analysis container
docker-compose stop lesson-analysis

REM 2. Remove the old container
docker-compose rm -f lesson-analysis

REM 3. Rebuild the image (no cache to pick up all code changes)
docker-compose build --no-cache lesson-analysis

REM 4. Start the updated container (detached)
docker-compose up -d lesson-analysis

REM 5. Follow the logs to verify it started correctly
docker-compose logs -f lesson-analysis
