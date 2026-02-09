# SEP490_AI

Repository that handles all tasks related to AI, PDF, Docx Converter

---

## Project Structure

```
SEP490_AI/
├── input_extraction/       # Worker service: extracts text from PDF/DOCX files
│   ├── main.py             # Entry point (CLI / future RabbitMQ mode)
│   ├── config.py           # Configuration loader (.env)
│   ├── gcs_handler.py      # Google Cloud Storage file downloader
│   ├── extractor.py        # Text extraction (pdfplumber & python-docx)
│   ├── worker.py           # Orchestrator: download → extract → output
│   ├── rabbitmq_consumer.py# Placeholder for future RabbitMQ integration
│   ├── requirements.txt    # Python dependencies
│   ├── .env.example        # Environment variable template
│   ├── credentials/        # GCS service account key (not committed)
│   └── temp_downloads/     # Temporary file storage during processing
└── README.md
```

---

## Services

### 1. Input Extraction Service (`input_extraction/`)

A worker service that downloads PDF/DOCX files from **Google Cloud Storage** and extracts all text content from them.

**Tech Stack:**
- Python 3.10+
- `pdfplumber` – PDF text extraction
- `python-docx` – DOCX text extraction
- `google-cloud-storage` – GCS file download
- `pika` – RabbitMQ client (future use)

**Current Mode:** Manual – pass file URIs via CLI  
**Future Mode:** Automatic – receives messages from RabbitMQ

#### Quick Start

1. **Install dependencies:**
   ```bash
   cd input_extraction
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   copy .env.example .env
   ```
   Edit `.env` and set:
   - `GCS_BUCKET_NAME` – your Google Cloud Storage bucket name
   - `GOOGLE_APPLICATION_CREDENTIALS` – path to your GCS service account JSON key

3. **Run:**
   ```bash
   # Full GCS URI
   python main.py gs://your-bucket/path/to/file.pdf

   # Blob path only (bucket name from .env)
   python main.py path/to/file.docx

   # Multiple files
   python main.py file1.pdf file2.docx
   ```

---

## Roadmap

- [x] Input Extraction Service (PDF & DOCX)
- [ ] RabbitMQ integration for automatic message-driven processing
- [ ] Docker containerization
- [ ] Additional AI-powered services (TBD)
