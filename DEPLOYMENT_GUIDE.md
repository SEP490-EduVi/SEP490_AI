# 🤖 EduVi AI Microservices Deployment Guide

This guide details the deployment pipeline for the **EduVi Platform's AI Workers**. The `.NET Backend (SEP490_BE)` and the `Python AI Microservices (SEP490_AI)` operate as a unified system on a single Google Cloud Platform (GCP) Virtual Machine. 

**This AI repository acts as the worker component provider.** It does not contain the production orchestrator (`docker-compose.yml`), which resides in the backend repository. 

---

## 🏗️ 1. Unified Architecture & The CI/CD Pipeline

When code is changed in an AI microservice (like `lesson_analysis` or `slide_generator`), the pipeline triggers selectively:

1. **Path-Filtered Trigger:** Pushing to `main` with changes only in a specific folder (e.g., `lesson_analysis/**`) triggers the respective GitHub Action.
2. **Build and Push:** The Action builds the Docker container based on the service's `Dockerfile` and pushes the artifact to GitHub Container Registry (GHCR) at `ghcr.io/sep490-eduvi/<service_name>`.
3. **Deploy:** The Action SSHs into the unified GCP Virtual Machine and runs `docker compose pull <service_name>` and `docker compose up -d`, seamlessly substituting the updated worker while the backend backend and database run uninterrupted.

---

## ⚙️ 2. Repository Configuration

All AI microservices have their own `.dockerignore` file to ensure secrets (like `private/gcp-key.json` or `.env`) are kept out of the Docker container during the build pipeline.

### GitHub Secrets Required
In **Settings > Secrets and variables > Actions**, these exactly formatted secrets are needed to push to the server:

| Secret Name | Value |
|---|---|
| `SERVER_HOST` | The external IP of the GCP VM (e.g., `34.87.16.235`) |
| `SERVER_USER` | The SSH username on the VM (e.g., `minhquang932004`) |
| `SERVER_SSH_KEY` | The raw private deployment SSH key content |

*(Note: The GHCR token is provided automatically by GitHub Actions).*

---

## 🖥️ 3. GCP VM Security & Setup

The production RabbitMQ broker, databases, and Python microservices run under a unified master `docker-compose.yml` on the GCP VM. 

### Google Cloud Authentication (No `gcp-key.json`)
The VM was provisioned with a **Google Service Account**. This automatically generates Application Default Credentials (ADC) for all Docker containers running on the host server. 

Because of this, **never push or manually copy `gcp-key.json` to the server.** The Python services automatically fall back to the VM metadata server to authenticate with Vertex AI, Gemini, and GCS.

### Editing Server Config via VS Code (Remote SSH)
Although the AI repositories are isolated, configuration parameters (such as `GOOGLE_CLOUD_PROJECT`, `NEO4J_URI`, and `HELICONE_API_KEY`) are managed in a globally shared `.env` file on the VM.

1. Install the **"Remote - SSH"** extension in VS Code.
2. Open the command palette (`Ctrl+Shift+P`) -> **Remote-SSH: Open SSH Configuration File**.
3. Add the server block:
   ```text
   Host eduvi-server
     HostName 34.87.16.235
     User minhquang932004
     IdentityFile C:\Users\<your_username>\.ssh\eduvi_deploy
   ```
4. Connect to `eduvi-server` and Open Folder: `/opt/eduvi/`
5. Here you can edit the global `.env` to apply shared AI variables (ensure `GITHUB_ORG=sep490-eduvi` is lowercase).
6. To apply changes made via the terminal:
   ```bash
   docker compose down
   docker compose up -d
   ```

---

## 🛡️ 4. Accessing Secure Internal Queues (SSH Tunneling)

Since the AI workers constantly process RabbitMQ jobs fed by the `.NET` backend, debugging the pipeline requires accessing the **RabbitMQ Dashboard** and the **Dozzle Logging Viewer**.

These ports are not exposed to the public internet on the GCP server. Teammates must use an SSH tunnel.

### SSH Tunnel Setup for Teammates:
1. Place your team's deployment private SSH key (`eduvi_deploy`) into `C:\Users\<username>\.ssh\`.
2. Fix permissions using PowerShell (to secure the key):
   ```powershell
   icacls "$env:USERPROFILE\.ssh\eduvi_deploy" /inheritance:r /grant:r "$env:USERNAME:R"
   ```
3. Create/Edit `config` at `C:\Users\<username>\.ssh\config`:
   ```text
   Host eduvi-tunnel
     HostName 34.87.16.235
     User minhquang932004
     IdentityFile C:\Users\<your_username>\.ssh\eduvi_deploy
     LocalForward 9999 127.0.0.1:9999
     LocalForward 8081 127.0.0.1:8081
     LocalForward 15672 127.0.0.1:15672
   ```
4. Start the tunnel in a PowerShell prompt (keep it open):
   ```powershell
   ssh -N eduvi-tunnel
   ```
5. View dashboards securely via browser:
   * **Monitor all AI logs (Dozzle):** [http://127.0.0.1:9999](http://127.0.0.1:9999)
   * **Inspect Task Queues (RabbitMQ):** [http://127.0.0.1:15672](http://127.0.0.1:15672) (User is `eduvi` + your `.env` password)
   * **Test Endpoints (Swagger):** [http://127.0.0.1:8081/swagger/index.html](http://127.0.0.1:8081/swagger/index.html)