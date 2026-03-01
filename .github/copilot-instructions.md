# Copilot Instructions — SEP490_AI

## Project Overview

This is the AI services repository for the **EduVi** platform. It processes lesson plans and textbook data for a Vietnamese education system. The backend is an ASP.NET application that communicates with these Python services via **RabbitMQ**.

## Architecture

```
ASP.NET Backend → RabbitMQ → lesson_analysis (Python worker)
                           → textbook_ingestion (Python worker, currently disabled)
```

- **lesson_analysis**: Consumes tasks from `lesson.analysis.requests`, processes lesson plans, publishes progress/results to `pipeline.results`.
- **textbook_ingestion**: Ingests textbook PDFs into Neo4j. This service **defines the data schema** in Neo4j — always check it first when working with Neo4j IDs or data formats.

## Critical: Neo4j ID Format

The `textbook_ingestion` service writes data to Neo4j. The ID format is defined in `textbook_ingestion/entity_generator/geography.py` and `textbook_ingestion/main.py`.

**ID construction chain:**
```
book_id = "{subject}_{grade}"           → e.g. "dia_li_lop_10"
part_id = "{book_id}_{P<number>}"       → e.g. "dia_li_lop_10_P1"
chapter_id = "{book_id}_{C<number>}"    → e.g. "dia_li_lop_10_C1"
lesson_id = "{book_id}_{L<number>}"     → e.g. "dia_li_lop_10_L1"
section_id = "{book_id}_{Sec_<n>.<m>}"  → e.g. "dia_li_lop_10_Sec_1.1"
```

**When the backend sends `lessonCode: "bai_1"`, it must be converted to `dia_li_lop_10_L1` using subject + grade.**

The conversion logic is in `lesson_analysis/pipeline.py` → `_build_full_lesson_id()`.

## RabbitMQ Message Formats

**Incoming** (from ASP.NET backend on `lesson.analysis.requests`):
```json
{
  "taskId": "uuid",
  "userId": "string",
  "productId": 7,
  "gcsUri": "gs://bucket/path/to/file.pdf",
  "subjectCode": "dia_li",
  "gradeCode": "10",
  "lessonCode": "bai_1"
}
```

**Outgoing** (to `pipeline.results`, consumed by ASP.NET):
```json
{
  "taskId": "uuid",
  "userId": "string",
  "productId": 7,
  "status": "processing|completed|failed",
  "step": "started|downloading|extracting_text|fetching_data|evaluating|completed|error",
  "progress": 0-100,
  "detail": "string|null",
  "result": "object|null",
  "error": "string|null"
}
```

## Neo4j Graph Schema

```
Book → Part → Chapter → Lesson → Section → Concept | Location | Figure
```

Lessons may sit directly under Part (no Chapter). Queries use exact match on node `id` property.

## Key Conventions

- **Language**: All textbook content, concepts, and locations are in **Vietnamese**. Preserve diacritics in content but strip them for matching (see `_strip_diacritics` in `neo4j_client.py`).
- **Subject code format**: snake_case Vietnamese without diacritics (e.g. `dia_li`, not `địa_lí`).
- **Grade code format**: Just the number (e.g. `"10"`), not `"lop_10"`. The `_build_full_lesson_id` function handles both.
- **LLM**: Uses Google Gemini via Vertex AI (model: `gemini-2.5-flash`).
- **File types**: Supports PDF and DOCX lesson plans from Google Cloud Storage.

## Docker

- All services run via `docker-compose.yml` in the project root.
- Use `redeploy.bat` to rebuild and restart the `lesson-analysis` container after code changes.
- See `DOCKER_GUIDE.txt` for full command reference.
- **Dozzle** (http://localhost:9999) for real-time log viewing.
- **RabbitMQ Management** (http://localhost:15672) for queue monitoring.

## Important Files

| File | Purpose |
|---|---|
| `lesson_analysis/pipeline.py` | Main processing pipeline + lesson ID resolution |
| `lesson_analysis/rabbitmq_utils.py` | Message publishing (progress + results) |
| `lesson_analysis/neo4j_client.py` | All Neo4j read queries |
| `lesson_analysis/evaluator.py` | LLM evaluation logic |
| `textbook_ingestion/entity_generator/geography.py` | Defines Neo4j schema + ID format |
| `textbook_ingestion/main.py` | `_make_book_id()` — generates book IDs |

## Common Pitfalls

1. **Always check textbook_ingestion first** when working with Neo4j data — it's the writer that defines the schema and ID format.
2. **Lesson IDs from the backend are short** (e.g. `bai_1`) and must be converted to full Neo4j IDs (e.g. `dia_li_lop_10_L1`).
3. **The Gemini API call dominates processing time** (~58s out of ~59s total). Everything else is sub-second.
4. **GCP credentials** are mounted from `private/gcp-key.json` — never commit this file.
