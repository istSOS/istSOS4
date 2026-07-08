# Utility di migrazione istSOS

Questa directory contiene due utility distinte:

- `istsos2_to_istsos4`: importa osservazioni da istSOS2 verso istSOS4;
- `istsos4_to_istsos4`: copia osservazioni tra due istanze istSOS4.

Le utility condividono [`client.py`](client.py), ma hanno Dockerfile,
dipendenze e immagini separate.

## Prerequisiti

- Docker;
- accesso di rete alle istanze sorgente e destinazione;
- datastream di destinazione già esistenti in istSOS4.

Tutti i comandi seguenti devono essere eseguiti da questa directory:

```bash
cd utils/istsos2istsos
```

I file `.env` contengono credenziali e non vengono inclusi nelle immagini.
Crearli partendo dai rispettivi esempi:

```bash
cp istsos2_to_istsos4/.env.example istsos2_to_istsos4/.env
cp istsos4_to_istsos4/.env.example istsos4_to_istsos4/.env
```

## Migrazione istSOS2 → istSOS4

### Configurazione

Compilare `istsos2_to_istsos4/.env`:

```dotenv
ISTSOS2_URL=https://source.example/istsos2
ISTSOS2_USER=source-user
ISTSOS2_PASSWORD=source-password

ISTSOS4_URL=http://localhost:8019/v4/v1.1
ISTSOS4_USER=target-user
ISTSOS4_PASSWORD=target-password
```

Il file `istsos2_to_istsos4/config.yml` definisce i job. Le procedure nelle due
liste vengono associate per posizione:

```yaml
istsos:
  continue_on_error: true
  imports:
    - name: example
      enabled: true
      service: sosraw
      step_days: 1
      procedures_istsos2:
        - SOURCE_PROCEDURE
      procedures_istsos4:
        - TARGET_DATASTREAM
```

`step_days` determina l'intervallo temporale letto da istSOS2 per ogni
richiesta. L'invio a istSOS4 viene comunque suddiviso in bulk sicuri.

### Build

```bash
docker build \
  -f istsos2_to_istsos4/Dockerfile \
  -t istsos2-to-istsos4:local \
  .
```

### Run

```bash
docker run --rm \
  --network host \
  --env-file istsos2_to_istsos4/.env \
  istsos2-to-istsos4:local
```

Per utilizzare un altro file di configurazione:

```bash
docker run --rm \
  --network host \
  --env-file istsos2_to_istsos4/.env \
  -v "$PWD/my-config.yml:/app/istsos2_to_istsos4/config.yml:ro" \
  istsos2-to-istsos4:local
```

## Migrazione istSOS4 → istSOS4

### Configurazione

Compilare `istsos4_to_istsos4/.env`:

```dotenv
ISTSOS4_FROM_URL=https://source.example/v1.1
ISTSOS4_FROM_USER=source-user
ISTSOS4_FROM_PASSWORD=source-password
NETWORK_FROM=
PROCEDURES_FROM=
TIMESTAMP_START_FROM=
TIMESTAMP_END_FROM=

ISTSOS4_TO_URL=http://localhost:8019/v4/v1.1
ISTSOS4_TO_USER=target-user
ISTSOS4_TO_PASSWORD=target-password
NETWORK_TO=

BATCH_SIZE=2000
```

I filtri sono opzionali:

- `NETWORK_FROM`: limita i datastream alla network sorgente;
- `PROCEDURES_FROM`: nomi dei datastream separati da virgola;
- `TIMESTAMP_START_FROM`: limite iniziale ISO 8601 incluso;
- `TIMESTAMP_END_FROM`: limite finale ISO 8601 incluso;
- `NETWORK_TO`: network nella quale cercare i datastream di destinazione;
- `BATCH_SIZE`: limite tecnico, massimo `2000`.

Se i timestamp sono vuoti vengono lette tutte le osservazioni. La migrazione
raggruppa i dati per giorno UTC e salta i `phenomenonTime` già presenti nella
destinazione.

### Build

```bash
docker build \
  -f istsos4_to_istsos4/Dockerfile \
  -t istsos4-to-istsos4:local \
  .
```

### Run

```bash
docker run --rm \
  --network host \
  --env-file istsos4_to_istsos4/.env \
  istsos4-to-istsos4:local
```

## Networking

Su Linux, `--network host` permette al container di raggiungere servizi
configurati come `localhost`, ad esempio `http://localhost:8019`.

Su Docker Desktop usare `host.docker.internal` negli URL al posto di
`localhost`; in quel caso `--network host` può essere omesso.

## Esecuzione senza Docker

Le utility restano eseguibili direttamente:

```bash
python3 istsos2_to_istsos4/istsos2istsos.py
python3 istsos4_to_istsos4/istsos2istsos.py
```
