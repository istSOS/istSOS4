# istsos4_to_istsos4

Copia osservazioni tra due istanze istSOS4, tramite la libreria
[istsos4-client](https://github.com/istSOS/istSOS4-client).

## Prerequisiti

- Docker;
- accesso di rete alle istanze sorgente e destinazione;
- datastream di destinazione già esistenti in istSOS4.

Tutti i comandi seguenti vanno eseguiti da questa directory:

```bash
cd utils/jobs/istsos4_to_istsos4
cp .env.example .env
```

`.env` contiene credenziali, resta locale (ignorato da git) e non viene incluso
nell'immagine.

## Configurazione

Compilare `.env`:

```dotenv
ISTSOS4_FROM_URL=https://source.example/v1.1
ISTSOS4_FROM_USER=source-user
ISTSOS4_FROM_PASSWORD=source-password
NETWORK_FROM=
DATASTREAMS_FROM=
TIMESTAMP_START_FROM=
TIMESTAMP_END_FROM=

ISTSOS4_TO_URL=http://localhost:8019/v4/v1.1
ISTSOS4_TO_USER=target-user
ISTSOS4_TO_PASSWORD=target-password
NETWORK_TO=
DATASTREAMS_TO=

IMPORT_NODATA=true
NODATA_VALUE=-999.9
```

I filtri sono opzionali:

- `NETWORK_FROM`: limita i datastream alla network sorgente;
- `DATASTREAMS_FROM`: nomi dei datastream sorgente separati da virgola;
- `TIMESTAMP_START_FROM`: limite iniziale ISO 8601 incluso;
- `TIMESTAMP_END_FROM`: limite finale ISO 8601 incluso;
- `NETWORK_TO`: network nella quale cercare i datastream di destinazione;
- `DATASTREAMS_TO`: nomi dei datastream di destinazione separati da virgola,
  associati per posizione a `DATASTREAMS_FROM`. Se vuoto, i nomi di
  destinazione sono uguali a quelli sorgente;
- `IMPORT_NODATA`: `true` (default) importa anche le osservazioni "no data",
  `false` le scarta;
- `NODATA_VALUE`: valore sentinella da scartare (default `-999.9`, confronto
  numerico), considerato solo quando `IMPORT_NODATA=false`.

Per copiare datastream con nomi diversi nelle due istanze:

```dotenv
DATASTREAMS_FROM=source_temperature,source_rain
DATASTREAMS_TO=target_temperature,target_rain
```

Le due liste devono contenere lo stesso numero di nomi.

L'invio a istSOS4 non richiede configurazione: il client suddivide
automaticamente ogni bulk in richieste che restano sotto il limite di parametri
del database di destinazione.

Se i timestamp sono vuoti vengono lette tutte le osservazioni. La migrazione
elabora le osservazioni a blocchi grandi quanto una singola insert ed esegue,
per ogni blocco, una sola query anti-duplicati che salta i `phenomenonTime` già
presenti nella destinazione.

## Build

```bash
docker build -t istsos4-to-istsos4:local .
```

## Run

```bash
docker run --rm \
  --network host \
  --env-file .env \
  istsos4-to-istsos4:local
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

Lo script legge `.env` dalla propria directory:

```bash
python3 istsos4_to_istsos4.py
```
