# istSOS4 Tutorial

This folder contains the python notebooks used in the tutorials.

# Git Workflow: Merging Branches

## Scopo

Questa guida spiega come importare (fare il merge) delle modifiche dal branch `traveltime` nel branch `traveltime_edu`.

## Passaggi

1. **Assicurati di essere sul branch `traveltime_edu`:**

   ```bash
   git checkout traveltime_edu
   ```

2. **Esegui il pull delle ultime modifiche da `traveltime`:**

   Prima di fare il merge, assicurati che il branch `traveltime` sia aggiornato:

   ```bash
   git checkout traveltime
   git pull origin traveltime
   ```

3. **Torna su `traveltime_edu`:**

   ```bash
   git checkout traveltime_edu
   ```

4. **Fai il merge delle modifiche da `traveltime` in `traveltime_edu`:**

   ```bash
   git merge traveltime
   ```

   Se ci sono conflitti, Git ti notificherà e dovrai risolverli manualmente. Dopo aver risolto eventuali conflitti, salva i file modificati e fai il commit:

   ```bash
   git add .
   git commit
   ```

5. **Pusha le modifiche (opzionale, se stai lavorando con un repository remoto):**

   ```bash
   git push origin traveltime_edu
   ```

Così facendo, avrai importato tutte le modifiche dal branch `traveltime` nel branch `traveltime_edu`.
