# Extensions après le parcours principal

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Ces expériences sont facultatives et se font sur le broker local. Aucun compte Aiven. Elles ne conditionnent pas la réussite de la journée. Résultat attendu : distinguer conservation, compaction et transaction. Vérification : sorties de commandes et assertions du script.

## Transactions Kafka


<!-- compose-pedagogie -->
**Avant de lancer :**

`run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`transactions` appelle [transactions](../src/experiments.ts) : créer un topic de test unique, annuler une publication, en valider une autre puis comparer read_uncommitted et read_committed. Les assertions contrôlent les résultats ; aucun offset des ateliers n’est réinitialisé.
**Code à lire — [src/experiments.ts](../src/experiments.ts#L10)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/experiments.ts:10-13 -->
```ts
let tx = await p.transaction();
await send(tx, topic, 'TKT-X', { status: 'aborted' }); await tx.abort();
tx = await p.transaction();
await send(tx, topic, 'TKT-Y', { status: 'committed' }); await tx.commit();
```


```sh
docker compose run --rm app transactions
```

Le script crée un topic unique, publie un record dans une transaction annulée et un autre dans une transaction validée. Résultat attendu : deux records pour `read_uncommitted`, un seul pour `read_committed`, puis `TRANSACTIONS OK`. La lecture du second groupe attend volontairement la fin de son délai pour montrer l'absence du record annulé.

Cette expérience prouve la visibilité commit/abort. Elle ne met pas en œuvre une transaction entre consommation, publication, offset et base externe. Pour un pipeline Kafka vers Kafka, étudier `transaction.sendOffsets({ consumer, topics })` avec les sorties ; cela ne rend pas un appel HTTP externe atomique. [Client JavaScript Confluent](https://docs.confluent.io/kafka-clients/javascript/current/overview.html).

## Observer une rétention courte

Créer uniquement ce topic d'extension avec des paramètres volontairement courts, puis publier.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.retention.v1`. `--partitions 1` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

`--if-not-exists` évite une erreur si le topic existe ; il ne remet pas sa configuration à la valeur de l’exemple.

Paramètres du topic : `retention.ms=10000`, `segment.ms=1000`. retention.ms et segment.ms sont en millisecondes ; le nettoyage reste asynchrone.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`seed orders` construit trois billets avec `makeEvent`, les valide puis appelle `send` vers `dev.lab.retention.v1`. `--solution` sélectionne [solutions/routing.ts](../solutions/routing.ts) pour la clé et [contracts/ticket-sold.v1.json](../contracts/ticket-sold.v1.json) pour le contrat ; aucun exercice à compléter.

Le nom historique `orders` désigne ce jeu de billets. `send` sérialise le JSON et attend la confirmation de publication ; aucun PDF ni email n’est créé par cette commande.
**Code à lire — [src/cli.ts](../src/cli.ts#L94)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:94-98 -->
```ts
const { eventKey } = flags.solution ? await import('../solutions/routing.ts') : await import('../exercises/routing.ts');
for (let i = 1; i <= 3; i++) {
  const event = makeEvent(i); validate(event, flags.solution ? 'contracts/ticket-sold.v1.json' : 'exercises/ticket-sold.v1.json');
  await send(p, flags.topic ?? MAIN, eventKey(event), event);
}
```


**Commande 3 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.retention.v1` avec le groupe `retention-before`, afficher les records et valider leur offset suivant. `--count 3` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

Cette lecture change les positions du groupe, pas la base SQLite. Un groupe déjà utilisé reprend sa position ; `fromBeginning: true` ne force pas le retour au début s’il possède un offset valide.
**Code à lire — [src/bus.ts](../src/bus.ts#L67)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:67-68 -->
```ts
const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
  fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
```


```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.retention.v1 --partitions 1 --replication-factor 1 --config retention.ms=10000 --config segment.ms=1000
docker compose run --rm app seed orders --solution --topic dev.lab.retention.v1
docker compose run --rm app read --topic dev.lab.retention.v1 --group retention-before --count 3
```

Après une période d'attente supérieure à la rétention, republier le jeu pour favoriser le roulement du segment, puis examiner l'offset bas conservé avec `app supervise --topic dev.lab.retention.v1 --group retention-before`. Répéter l'observation si nécessaire. Le segment actif, son roulement et le nettoyage asynchrone empêchent de promettre une suppression exactement après dix secondes. Une nouvelle lecture ne retrouve que les records encore conservés. Ne pas utiliser cette rétention pour les ateliers principaux.

## Étudier un journal compacté


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.compacted.v1`. `--partitions 1` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

`--if-not-exists` évite une erreur si le topic existe ; il ne remet pas sa configuration à la valeur de l’exemple.

Paramètres du topic : `cleanup.policy=compact`, `segment.ms=1000`, `min.cleanable.dirty.ratio=0.01`. retention.ms et segment.ms sont en millisecondes ; le nettoyage reste asynchrone.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`seed orders` construit trois billets avec `makeEvent`, les valide puis appelle `send` vers `dev.lab.compacted.v1`. `--solution` sélectionne [solutions/routing.ts](../solutions/routing.ts) pour la clé et [contracts/ticket-sold.v1.json](../contracts/ticket-sold.v1.json) pour le contrat ; aucun exercice à compléter.

Le nom historique `orders` désigne ce jeu de billets. `send` sérialise le JSON et attend la confirmation de publication ; aucun PDF ni email n’est créé par cette commande.

**Commande 3 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`seed orders` construit trois billets avec `makeEvent`, les valide puis appelle `send` vers `dev.lab.compacted.v1`. `--solution` sélectionne [solutions/routing.ts](../solutions/routing.ts) pour la clé et [contracts/ticket-sold.v1.json](../contracts/ticket-sold.v1.json) pour le contrat ; aucun exercice à compléter.

Le nom historique `orders` désigne ce jeu de billets. `send` sérialise le JSON et attend la confirmation de publication ; aucun PDF ni email n’est créé par cette commande.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.compacted.v1 --partitions 1 --replication-factor 1 --config cleanup.policy=compact --config segment.ms=1000 --config min.cleanable.dirty.ratio=0.01
docker compose run --rm app seed orders --solution --topic dev.lab.compacted.v1
docker compose run --rm app seed orders --solution --topic dev.lab.compacted.v1
```

Les mêmes clés sont écrites deux fois. Le nettoyage pourra progressivement éliminer leurs anciennes valeurs dans les segments éligibles. Observer avec un groupe neuf ; la conservation immédiate de six records n'est pas un échec, le cleaner n'est pas synchrone. Un journal compacté aide à reconstruire un dernier état par clé ; il ne garantit pas de conserver chaque changement. Les tombstones et durées de suppression demandent une expérience dédiée avant usage réel. [Design et compaction Apache Kafka](https://kafka.apache.org/41/design/design/).

## Observer les traces

Suivre l'extension console de la [fiche Aiven et observabilité](Aiven-et-observabilite.md). Aucun exporteur cloud ni dashboard n'est nécessaire pour constater la propagation d'un identifiant de trace.
