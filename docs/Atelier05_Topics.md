# Atelier 5 — Configurer et superviser un topic

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Vous préparez un flux de formation. L'objectif est de comprendre les paramètres que l'équipe applicative doit demander ou contrôler sur un service managé, sans installer ni administrer les brokers. Livrable : topic de laboratoire décrit et lag interprété.

Préparation : Compose démarré, Kafka 4.1.2 et outils inclus dans son image ; Node.js 24.14.0, client Confluent `@confluentinc/kafka-javascript` 1.11.0. Aucun compte Aiven ; les commandes ne concernent que le broker local. Un terminal suffit. Les contrôles sont dans les outils, aucun fichier de notes n'est requis.

## 1. Préparer le topic de laboratoire

### 1.1. Créer dev.lab.infra.v1 avec une partition et une rétention d'un jour

- **A5-1.1.1** Créer le topic. Résultat attendu : `Created topic dev.lab.infra.v1.` L'avertissement qui le précède sur les points et les tirets bas dans les noms de topics est normal : nos noms n'utilisent que des points. `--if-not-exists` permet de relancer sans recréer un topic existant ; il ne restaure pas ses paramètres.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.infra.v1`. `--partitions 1` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

`--if-not-exists` évite une erreur si le topic existe ; il ne remet pas sa configuration à la valeur de l’exemple.

Paramètres du topic : `retention.ms=86400000`. retention.ms et segment.ms sont en millisecondes ; le nettoyage reste asynchrone.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.infra.v1`. 

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.infra.v1 --partitions 1 --replication-factor 1 --config retention.ms=86400000
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.infra.v1
```

- **A5-1.1.2** Vérifier une partition et un facteur de réplication de 1 dans l'environnement neuf. Interprétation : c'est une simplification locale ; une seule copie ne supporte pas la perte du broker. Sur Aiven, les choix de service et de réplication relèvent du service souscrit et de la plateforme, pas d'une installation par les développeurs.

### 1.2. Étendre à trois partitions et demander sept jours de rétention

- **A5-1.2.1** Modifier uniquement ce topic de laboratoire, puis le décrire.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` augmente le nombre de partitions. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.infra.v1`. `--partitions 3` règle les partitions ; 

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

`kafka-configs.sh` cible la configuration du topic `dev.lab.infra.v1`. `--alter --add-config` modifie la politique indiquée ; les anciennes données ne sont pas immédiatement effacées. Il s’agit d’un outil officiel de Kafka, sans code TypeScript local à modifier.

**Commande 3 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.infra.v1`. 

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 4 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

`kafka-configs.sh` cible la configuration du topic `dev.lab.infra.v1`. `--describe` affiche les paramètres du topic. Il s’agit d’un outil officiel de Kafka, sans code TypeScript local à modifier.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic dev.lab.infra.v1 --partitions 3
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name dev.lab.infra.v1 --alter --add-config retention.ms=604800000
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.infra.v1
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name dev.lab.infra.v1 --describe
```

- **A5-1.2.2** Vérifier trois partitions et `retention.ms=604800000`. Interprétation : les anciens records ne sont pas redistribués ; les nouvelles clés peuvent changer de partition après l'augmentation. La diminution du nombre de partitions n'est pas une opération symétrique. La rétention s'applique par segments et nettoyage asynchrone : elle ne promet pas une suppression à la milliseconde.

## 2. Observer les positions de lecture

### 2.1. Publier trois records puis lire partiellement le groupe de supervision

- **A5-2.1.1** Publier une seule fois le jeu fourni avec le contrat de référence. Cette variante ne dépend pas des modifications de l'atelier développeur.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`seed orders` construit trois billets avec `makeEvent`, les valide puis appelle `send` vers `dev.lab.infra.v1`. `--solution` sélectionne [solutions/routing.ts](../solutions/routing.ts) pour la clé et [contracts/ticket-sold.v1.json](../contracts/ticket-sold.v1.json) pour le contrat ; aucun exercice à compléter.

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


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.infra.v1` avec le groupe `supervision-A5`, afficher les records et valider leur offset suivant. `--count 1` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

Cette lecture change les positions du groupe, pas la base SQLite. Un groupe déjà utilisé reprend sa position ; `fromBeginning: true` ne force pas le retour au début s’il possède un offset valide.
**Code à lire — [src/bus.ts](../src/bus.ts#L67)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:67-68 -->
```ts
const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
  fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
```


**Commande 3 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`supervise` appelle [supervise](../src/supervision.ts) pour `dev.lab.infra.v1` et le groupe `supervision-A5`. L’Admin API lit l’offset bas conservé, la fin du journal et les positions validées. Cette commande ne consomme pas les messages, ne valide aucun nouvel offset et ne crée aucun PDF.

Dans le tableau affiché, le lag est log_end moins committed_next ; la position -1 est rendue par « offset absent » au lieu de produire un faux retard numérique.
**Code à lire — [src/supervision.ts](../src/supervision.ts#L6)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/supervision.ts:6-8 -->
```ts
const ends = await admin.fetchTopicOffsets(topic);
const committed = await admin.fetchOffsets({ groupId: group, topics: [topic] });
const positions = committed.find(t => t.topic === topic)?.partitions ?? [];
```


```sh
docker compose run --rm app seed orders --solution --topic dev.lab.infra.v1
docker compose run --rm app read --topic dev.lab.infra.v1 --group supervision-A5 --count 1
docker compose run --rm app supervise --topic dev.lab.infra.v1 --group supervision-A5
```

- **A5-2.1.2** Vérifier que le groupe n'a pas tout lu. Certaines partitions peuvent afficher `offset absent` parce qu'aucun offset n'y a été committé. Un lag numérique compare l'offset de fin et le prochain offset validé dans une même partition. Ce nombre n'est ni un temps de retard ni une preuve que l'effet métier a réussi.

### 2.2. Terminer la lecture et vérifier que les offsets avancent

- **A5-2.2.1** Lire les deux records restants avec le même groupe, puis contrôler à nouveau.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.infra.v1` avec le groupe `supervision-A5`, afficher les records et valider leur offset suivant. `--count 2` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.
**Code à lire — [src/bus.ts](../src/bus.ts#L92)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:92-93 -->
```ts
await c.commitOffsets([{ topic: payload.topic, partition: payload.partition,
  offset: (BigInt(payload.message.offset) + 1n).toString() }]);
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`supervise` appelle [supervise](../src/supervision.ts) pour `dev.lab.infra.v1` et le groupe `supervision-A5`. L’Admin API lit l’offset bas conservé, la fin du journal et les positions validées. Cette commande ne consomme pas les messages, ne valide aucun nouvel offset et ne crée aucun PDF.

Dans le tableau affiché, le lag est log_end moins committed_next ; la position -1 est rendue par « offset absent » au lieu de produire un faux retard numérique.

```sh
docker compose run --rm app read --topic dev.lab.infra.v1 --group supervision-A5 --count 2
docker compose run --rm app supervise --topic dev.lab.infra.v1 --group supervision-A5
```

- **A5-2.2.2** Résultat attendu : trois records lus au total. Les partitions ayant reçu des records et désormais committées ont un lag nul ; une partition vide peut conserver `offset absent`. Vérifier les sorties. Le contrôle du topic et l'interprétation du lag constituent la preuve de réussite.

En cas de publications relancées, les totaux changent ; un groupe neuf repart du début conservé. Sur Aiven, traduire ces contrôles dans la console ou les outils autorisés, avec les droits disponibles.
