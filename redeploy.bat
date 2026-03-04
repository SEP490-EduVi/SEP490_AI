@echo off
REM ============================================================
REM  Redeploy AI services after code changes
REM  Usage: redeploy.bat [service]
REM    service: lesson-analysis | slide-generator | all (default: all)
REM  Run from: D:\SEP490\SEP490_AI
REM ============================================================

SET SERVICE=%1
IF "%SERVICE%"=="" SET SERVICE=all

IF "%SERVICE%"=="lesson-analysis" GOTO deploy_lesson
IF "%SERVICE%"=="slide-generator" GOTO deploy_slide
IF "%SERVICE%"=="all" GOTO deploy_all

ECHO Unknown service: %SERVICE%
ECHO Valid options: lesson-analysis, slide-generator, all
EXIT /B 1

:deploy_lesson
ECHO === Redeploying lesson-analysis ===
docker-compose stop lesson-analysis
docker-compose rm -f lesson-analysis
docker-compose build --no-cache lesson-analysis
docker-compose up -d lesson-analysis
docker-compose logs -f lesson-analysis
GOTO end

:deploy_slide
ECHO === Redeploying slide-generator ===
docker-compose stop slide-generator
docker-compose rm -f slide-generator
docker-compose build --no-cache slide-generator
docker-compose up -d slide-generator
docker-compose logs -f slide-generator
GOTO end

:deploy_all
ECHO === Redeploying all AI services ===
docker-compose stop lesson-analysis slide-generator
docker-compose rm -f lesson-analysis slide-generator
docker-compose build --no-cache lesson-analysis slide-generator
docker-compose up -d lesson-analysis slide-generator
docker-compose logs -f lesson-analysis slide-generator
GOTO end

:end
