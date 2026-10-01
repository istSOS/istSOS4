# istSOS4 utilities

Tools that move data into istSOS4. Each folder is one tool, one Docker image
and its own build context (`docker build -t <image> <folder>`).

- `services/` run continuously: `docker compose up -d`.
- `jobs/` run once and exit: `docker run --rm`, by hand or from cron.

| Tool | Kind | Run |
| --- | --- | --- |
| [mqtt2istsos](services/mqtt2istsos/) | service | `docker compose up -d` |
| [ftp2istsos](jobs/ftp2istsos/) | job, cron | `./run.sh` |
| [eyeonwater2istsos](jobs/eyeonwater2istsos/) | job, cron or manual | `./fetch.sh`, `./run.sh FILE.json` |
| [xlsx2istsos](jobs/xlsx2istsos/) | job, manual | `./run.sh FILE.xlsx` |
| [istsos2_to_istsos4](jobs/istsos2_to_istsos4/) | job, one-off migration | `docker run`, see its README |
| [istsos4_to_istsos4](jobs/istsos4_to_istsos4/) | job, one-off migration | `docker run`, see its README |

## Conventions

- A folder holds its `Dockerfile`, `requirements.txt`, `README.md` and a
  tracked `*.example` config. The real `.env` and `config.yaml` stay local
  (gitignored) and are passed in at run time, never copied into the image.
- No code is shared between folders: istSOS4 access goes through
  [istsos4-client](https://github.com/istSOS/istSOS4-client).
- A job has a `run.sh` only when it does more than `docker run`: mounting an
  input file, computing dates, being a cron target.
- Published images are named `ghcr.io/istsos/istsos4/utils/<tool>:<version>`.
- Tests (`test_*.py`) sit next to the code and stay out of the images.
