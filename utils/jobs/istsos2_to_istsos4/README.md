# istsos2_to_istsos4

Importa osservazioni da istSOS2 verso datastream già esistenti in istSOS4.
Parla con istSOS4 tramite la libreria
[istsos4-client](https://github.com/istSOS/istSOS4-client);
[`istsos2_client.py`](istsos2_client.py) legge le API legacy di istSOS2.

## Prerequisiti

- Docker;
- accesso di rete alle istanze sorgente e destinazione;
- datastream di destinazione già esistenti in istSOS4.

Tutti i comandi seguenti vanno eseguiti da questa directory:

```bash
cd utils/jobs/istsos2_to_istsos4
cp .env.example .env
cp config.example.yaml config.yaml
```

`.env` e `config.yaml` restano locali (ignorati da git) e non vengono inclusi
nell'immagine.

## Configurazione

Compilare `.env`:

```dotenv
ISTSOS2_URL=https://source.example/istsos2
ISTSOS2_USER=source-user
ISTSOS2_PASSWORD=source-password

ISTSOS4_URL=http://localhost:8019/v4/v1.1
ISTSOS4_USER=target-user
ISTSOS4_PASSWORD=target-password

IMPORT_NODATA=true
NODATA_VALUE=-999.9
```

`IMPORT_NODATA` decide se importare anche le osservazioni "no data": `true`
(default) le importa, `false` le scarta. `NODATA_VALUE` è il valore sentinella
da scartare (default `-999.9`, confrontato numericamente) e viene considerato
solo quando `IMPORT_NODATA=false`.

`config.yaml` definisce i job. Le procedure nelle due liste vengono associate
per posizione:

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
richiesta e va tarato sulla frequenza del dato (valori piccoli per procedure ad
alta frequenza, più ampi per quelle rade). È indipendente dalla scrittura: il
client suddivide comunque l'invio a istSOS4 in lotti che restano sotto il limite
di parametri del database di destinazione.

## Build

```bash
docker build -t istsos2-to-istsos4:local .
```

## Run

`config.yaml` viene montato a runtime: modificarlo non richiede una nuova build.

```bash
docker run --rm \
  --network host \
  --env-file .env \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  istsos2-to-istsos4:local
```

## Log

Il modulo `logging` scrive timestamp e livello. La verbosità si regola con la
variabile `LOG_LEVEL` (`DEBUG`, `INFO` di default, `WARNING`, `ERROR`). A
livello `DEBUG` vengono mostrati anche i dettagli delle richieste HTTP e gli
eventi di autenticazione (refresh del token, re-login dopo un `401`,
suddivisione di un bulk troppo grande).

## Networking

Su Linux, `--network host` permette al container di raggiungere servizi
configurati come `localhost`, ad esempio `http://localhost:8019`.

Su Docker Desktop usare `host.docker.internal` negli URL al posto di
`localhost`; in quel caso `--network host` può essere omesso.

## Esecuzione senza Docker

Lo script legge `.env` e `config.yaml` dalla propria directory:

```bash
python3 istsos2_to_istsos4.py
```
