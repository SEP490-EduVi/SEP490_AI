# SEP490_AI

AI services for the EduVi platform — lesson plan analysis, textbook ingestion, and more.

---

## Project Structure

```
SEP490_AI/
├── docker-compose.yml          # All services + infrastructure
├── redeploy.bat                # Quick rebuild & restart script
├── DOCKER_GUIDE.txt            # Docker command reference
├── lesson_analysis/            # Worker: analyzes lesson plans against textbook standards
│   ├── main.py                 # Entry point (RabbitMQ consumer + CLI mode)
│   ├── config.py               # Configuration loader (.env)
│   ├── pipeline.py             # Orchestrator: download → extract → evaluate
│   ├── rabbitmq_utils.py       # RabbitMQ connection, queue declaration, progress publishing
│   ├── gcs_handler.py          # Google Cloud Storage file downloader
│   ├── extractor.py            # Text extraction (PDF/DOCX)
│   ├── evaluator.py            # LLM-based lesson plan evaluation (Gemini)
│   ├── neo4j_client.py         # Neo4j queries (books, lessons, concepts, locations)
│   ├── Dockerfile              # Container build definition
│   └── requirements.txt        # Python dependencies
├── textbook_ingestion/         # Worker: ingests textbook data into Neo4j (disabled)
│   ├── main.py
│   ├── pipeline.py
│   ├── chunker.py
│   ├── extractor.py
│   ├── keyword_extractor.py
│   ├── neo4j_loader.py
│   ├── gcs_handler.py
│   └── entity_generator/       # Subject-specific entity generators
├── private/                    # Credentials (not committed)
│   └── gcp-key.json
└── test/                       # Manual test scripts
    ├── test_neo4j.py
    └── test_vertex_ai.py
```

---

## Architecture

The system communicates with the ASP.NET backend via **RabbitMQ**:

1. Backend publishes a task message to `lesson.analysis.requests`
2. The lesson-analysis worker picks it up, processes it, and publishes progress/results to `pipeline.results`
3. Backend consumes results and pushes them to the frontend via SignalR

### Message Formats

**Incoming message** (from backend):
```json
{
  "taskId": "546de3de-e4c7-47fa-b6cd-9c6bad1e1d0b",
  "userId": "5",
  "productId": 7,
  "gcsUri": "gs://bucket/path/to/file.pdf",
  "subjectCode": "dia_li",
  "gradeCode": "10",
  "lessonCode": "dia_li_10_bai_1"
}
```

**Outgoing message** (progress updates & final result):
```json
{
  "taskId": "546de3de-e4c7-47fa-b6cd-9c6bad1e1d0b",
  "userId": "5",
  "productId": 7,
  "status": "completed",
  "step": "completed",
  "progress": 100,
  "detail": null,
  "result": { "..." },
  "error": null
}
```

Status values: `processing` → `completed` | `failed`

---

## Services (Docker Compose)

| Service | Port | Description |
|---|---|---|
| **lesson-analysis** | — | Lesson plan analysis worker (RabbitMQ consumer) |
| **rabbitmq** | `5672` / `15672` | Message broker + [Management UI](http://localhost:15672) (`guest`/`guest`) |
| **redis** | `6379` | Cache (future use) |
| **dozzle** | `9999` | [Real-time log viewer](http://localhost:9999) for all containers |

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- A `.env` file in the project root with:
  ```
  NEO4J_URI=neo4j+s://xxxxx.databases.neo4j.io
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=your_password
  GCS_BUCKET_NAME=your_bucket
  GOOGLE_CLOUD_PROJECT=your_project
  ```
- GCP service account key at `private/gcp-key.json`

### Start everything
```bash
docker-compose up -d
```

### After code changes
```bash
.\redeploy.bat
```

### View logs
```bash
# Terminal
docker-compose logs -f lesson-analysis

# Browser
# http://localhost:9999  (Dozzle)
```

### Stop everything
```bash
docker-compose down
```

> See `DOCKER_GUIDE.txt` for the full command reference.

---

## Local Development (without Docker)

Run only infrastructure in Docker, then run Python on your host:

```bash
# Start RabbitMQ + Redis only
docker-compose up -d rabbitmq redis

# Run the worker locally
cd lesson_analysis
pip install -r requirements.txt
python main.py
```

### CLI Mode (no RabbitMQ needed)
```bash
python main.py --cli <gcs_uri> <subject> <grade> [lesson_id]
```

---

## Lesson Analysis Pipeline

1. **Download** — fetch the lesson plan file from GCS
2. **Extract text** — parse PDF or DOCX into raw text
3. **Resolve lesson** — match to a Neo4j lesson (by ID or via LLM identification)
4. **Fetch standard data** — load concepts, locations, and sections from Neo4j
5. **Evaluate** — use Gemini to compare the lesson plan against the curriculum standards
6. **Return result** — publish evaluation result back via RabbitMQ

---

## Roadmap

- [x] Lesson Analysis Service (PDF & DOCX)
- [x] RabbitMQ integration for message-driven processing
- [x] Docker containerization
- [x] Dozzle log viewer for monitoring
- [ ] Textbook Ingestion Service (currently disabled)
- [ ] Additional AI-powered services (TBD)
