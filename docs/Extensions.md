# Extensions après le parcours principal

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Ces expériences sont facultatives et se font sur le broker local. Aucun compte Aiven. Elles ne conditionnent pas la réussite de la journée. Résultat attendu : distinguer conservation, compaction et transaction. Vérification : sorties de commandes et assertions du script.

## Transactions Kafka

```sh
docker compose run --rm app transactions
```

Le script crée un topic unique, publie un record dans une transaction annulée et un autre dans une transaction validée. Résultat attendu : deux records pour `read_uncommitted`, un seul pour `read_committed`, puis `TRANSACTIONS OK`. La lecture du second groupe attend volontairement la fin de son délai pour montrer l'absence du record annulé.

Cette expérience prouve la visibilité commit/abort. Elle ne met pas en œuvre une transaction entre consommation, publication, offset et base externe. Pour un pipeline Kafka vers Kafka, étudier `transaction.sendOffsets({ consumer, topics })` avec les sorties ; cela ne rend pas un appel HTTP externe atomique. [Client JavaScript Confluent](https://docs.confluent.io/kafka-clients/javascript/current/overview.html).

## Observer une rétention courte

Créer uniquement ce topic d'extension avec des paramètres volontairement courts, puis publier.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.retention.v1 --partitions 1 --replication-factor 1 --config retention.ms=10000 --config segment.ms=1000
docker compose run --rm app seed orders --solution --topic dev.lab.retention.v1
docker compose run --rm app read --topic dev.lab.retention.v1 --group retention-before --count 3
```

Après une période d'attente supérieure à la rétention, republier le jeu pour favoriser le roulement du segment, puis examiner l'offset bas conservé avec `app supervise --topic dev.lab.retention.v1 --group retention-before`. Répéter l'observation si nécessaire. Le segment actif, son roulement et le nettoyage asynchrone empêchent de promettre une suppression exactement après dix secondes. Une nouvelle lecture ne retrouve que les records encore conservés. Ne pas utiliser cette rétention pour les ateliers principaux.

## Étudier un journal compacté

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.compacted.v1 --partitions 1 --replication-factor 1 --config cleanup.policy=compact --config segment.ms=1000 --config min.cleanable.dirty.ratio=0.01
docker compose run --rm app seed orders --solution --topic dev.lab.compacted.v1
docker compose run --rm app seed orders --solution --topic dev.lab.compacted.v1
```

Les mêmes clés sont écrites deux fois. Le nettoyage pourra progressivement éliminer leurs anciennes valeurs dans les segments éligibles. Observer avec un groupe neuf ; la conservation immédiate de six records n'est pas un échec, le cleaner n'est pas synchrone. Un journal compacté aide à reconstruire un dernier état par clé ; il ne garantit pas de conserver chaque changement. Les tombstones et durées de suppression demandent une expérience dédiée avant usage réel. [Design et compaction Apache Kafka](https://kafka.apache.org/41/design/design/).

## Observer les traces

Suivre l'extension console de la [fiche Aiven et observabilité](Aiven-et-observabilite.md). Aucun exporteur cloud ni dashboard n'est nécessaire pour constater la propagation d'un identifiant de trace.
