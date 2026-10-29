# Atelier 7 — Découpler les retries avec un topic de reprise

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Le retry local attend pendant la panne. Vous testez une seconde organisation : envoyer le fait dans un topic de reprise, puis mettre en quarantaine après trois tentatives totales. Livrable : deux étapes de traitement contrôlées et leur état final. Les retry queues et dead-letter queues sont ici représentées par des topics Kafka.

Environnement : socle local opérationnel, Kafka 4.1.2, Node.js 24.14.0, client Confluent `@confluentinc/kafka-javascript` 1.11.0, code `src/queues.ts`. Les topics `dev.lab.queues.*` isolent cet atelier. Un seul worker actif à la fois utilise la base de l'exercice. Les décisions se contrôlent dans les outils, sans nouveau fichier de notes.

## 1. Traiter l'entrée sans retry local

### 1.1. Publier le jeu queues et router les pannes transitoires

- **A7-1.1.1** Publier une seule fois les cinq records puis traiter la source. Le worker tente chaque événement une fois et attend l'ACK de la sortie avant de committer l'entrée.

```sh
docker compose run --rm app seed resilience --topic dev.lab.queues.v1
docker compose run --rm app queue-worker --topic dev.lab.queues.v1 --group queues-source-A7 --db /state/queues-A7.db --count 5
```

- **A7-1.1.2** Vérifier `records=5`, `accepted=1`, `duplicate=1`, `retry_queued=2`, `dlq=1`. Interprétation : deux pannes transitoires vont en reprise ; la quantité invalide va directement en DLQ. Le même fait conserve `event_id` et sa clé lors du transfert.

## 2. Épuiser les reprises de manière bornée

### 2.1. Consommer le topic retry avec la même base et un compteur de tentative

- **A7-2.1.1** Lancer le worker sur le topic retry avec un autre groupe et la même base. Il lit deux records de tentative 2, puis les deux records de tentative 3 qu'il a republiés. La borne porte sur les tentatives métier de ce parcours, pas sur les retries réseau internes du client Kafka.

```sh
docker compose run --rm app queue-worker --topic dev.lab.queues.retry.v1 --group queues-retry-A7 --db /state/queues-A7.db --count 4
docker compose run --rm --no-deps app status --db /state/queues-A7.db
docker compose run --rm app read --topic dev.lab.queues.dlq.v1 --group queues-audit-A7 --count 2
```

- **A7-2.1.2** Vérifier pour cette seconde exécution `records=4`, `accepted=1`, `duplicate=0`, `retry_queued=2`, `dlq=1`. La base finale contient deux PDF. La DLQ contient une donnée invalide et une panne encore présente à la troisième tentative. Les comptes supposent une seule publication du jeu.

### 2.2. Lire le routage et identifier la perte d'ordre possible

- **A7-2.2.1** Ouvrir `src/queues.ts`. Repérer `attempt`, `retryTopic`, le test `attempt < 3`, l'ACK puis le commit. Résultat attendu : comprendre pourquoi une panne permanente évite le retry et comment une panne transitoire finit en DLQ.
- **A7-2.2.2** Comparer au cas « mise à jour client en erreur puis mise à jour suivante réussie ». Référence : sortir le premier fait vers un autre topic permet au suivant de passer ; l'ordre métier peut être inversé malgré une même clé. Choisir explicitement version métier, rejet des événements obsolètes ou blocage de clé/partition si l'ordre strict est indispensable.
- **A7-2.2.3** Vérifier la limite du délai : les 200 ms du worker retry sont pédagogiques ; Kafka ne rend pas lui-même un record visible à une date programmée. Un délai de plusieurs minutes exige une stratégie de scheduling, avec contrôle des heartbeats et de la charge. Publication en reprise et commit ne sont pas transactionnels ici ; un incident peut dupliquer la sortie, d'où la conservation de l'identité du fait.

## 3. Préparer un retraitement de quarantaine

### 3.1. Choisir la correction d'une donnée sans boucler dans la DLQ

- **A7-3.1.1** Pour la quantité négative, comparer les options à la référence : retrouver la source, corriger la cause, convenir d'un fait correctif ou d'un retraitement encadré, conserver la traçabilité de l'original et vérifier les effets existants. Ne pas vider automatiquement la DLQ vers l'entrée sans correction ni borne.
- **A7-3.1.2** Contrôler les résultats et leurs limites. L'atelier est réussi si vous identifiez succès, doublon, erreur définitive et panne persistante dans les traces et la DLQ. La recette `app integration` automatise ces assertions sur des topics isolés.
