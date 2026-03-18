@echo off
REM ============================================================
REM  Redeploy AI services after code changes
REM  Usage: redeploy.bat [service]
REM    service: lesson-analysis | slide-generator | curriculum-ingestion | all (default: all)
REM  Run from: D:\SEP490\SEP490_AI
REM ============================================================

SET SERVICE=%1
IF "%SERVICE%"=="" SET SERVICE=all

IF "%SERVICE%"=="lesson-analysis" GOTO deploy_lesson
IF "%SERVICE%"=="slide-generator" GOTO deploy_slide
IF "%SERVICE%"=="curriculum-ingestion" GOTO deploy_curriculum
IF "%SERVICE%"=="all" GOTO deploy_all

ECHO Unknown service: %SERVICE%
ECHO Valid options: lesson-analysis, slide-generator, curriculum-ingestion, all
EXIT /B 1

:deploy_lesson
ECHO === Redeploying lesson-analysis ===
docker-compose -f docker-compose.dev.yml stop lesson-analysis
docker-compose -f docker-compose.dev.yml rm -f lesson-analysis
docker-compose -f docker-compose.dev.yml build --no-cache lesson-analysis
docker-compose -f docker-compose.dev.yml up -d lesson-analysis
docker-compose -f docker-compose.dev.yml logs -f lesson-analysis
GOTO end

:deploy_slide
ECHO === Redeploying slide-generator ===
docker-compose -f docker-compose.dev.yml stop slide-generator
docker-compose -f docker-compose.dev.yml rm -f slide-generator
docker-compose -f docker-compose.dev.yml build --no-cache slide-generator
docker-compose -f docker-compose.dev.yml up -d slide-generator
docker-compose -f docker-compose.dev.yml logs -f slide-generator
GOTO end

:deploy_curriculum
ECHO === Redeploying curriculum-ingestion ===
docker-compose -f docker-compose.dev.yml stop curriculum-ingestion
docker-compose -f docker-compose.dev.yml rm -f curriculum-ingestion
docker-compose -f docker-compose.dev.yml build --no-cache curriculum-ingestion
docker-compose -f docker-compose.dev.yml up -d curriculum-ingestion
docker-compose -f docker-compose.dev.yml logs -f curriculum-ingestion
GOTO end

:deploy_all
ECHO === Redeploying all AI services ===
docker-compose -f docker-compose.dev.yml stop lesson-analysis slide-generator curriculum-ingestion
docker-compose -f docker-compose.dev.yml rm -f lesson-analysis slide-generator curriculum-ingestion
docker-compose -f docker-compose.dev.yml build --no-cache lesson-analysis slide-generator curriculum-ingestion
docker-compose -f docker-compose.dev.yml up -d lesson-analysis slide-generator curriculum-ingestion
docker-compose -f docker-compose.dev.yml logs -f lesson-analysis slide-generator curriculum-ingestion
GOTO end

:end
