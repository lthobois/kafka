# Atelier 2 — Publier des ventes et observer leur progression

Objectif : créer un topic à trois partitions, publier trois billets avec le contrat fourni puis observer les offsets d'un groupe. Le topic `dev.lab.design.v1` isole cette expérience. Le fait publié reste `ticket.sold` ; le nom de production de référence est `dev.ticketing.ticket.sold.v1`.

Prérequis : [README](../README.md) terminé ; l'atelier 1 n'est pas nécessaire pour les commandes. Environnement : même VM Linux, Docker, Kafka 4.1.2 et client Node.js fournis. Un terminal suffit. Aucun fichier ni code à rédiger ; `--solution` sélectionne le contrat et la clé prêts à l'emploi. Les références **A2-thématique.action.tâche** identifient les tâches.

## 1. Créer le journal des ventes

### 1.1. Créer dev.lab.design.v1 avec trois partitions et sept jours de rétention

- **A2-1.1.1** Créer le topic puis afficher sa configuration. Les trois partitions permettent de distribuer les clés ; RF1 est une simplification du laboratoire.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.design.v1`. `--partitions 3` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

`--if-not-exists` évite une erreur si le topic existe ; il ne remet pas sa configuration à la valeur de l’exemple.

Paramètres du topic : `retention.ms=604800000`. retention.ms et segment.ms sont en millisecondes ; le nettoyage reste asynchrone.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.design.v1`. 

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

```sh
cd ~/kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.design.v1 --partitions 3 --replication-factor 1 --config retention.ms=604800000
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.design.v1
```

- **A2-1.1.2** Vérifier `PartitionCount: 3`, `ReplicationFactor: 1` et `retention.ms=604800000`. Sept jours est une valeur de laboratoire ; RF1 ne protège pas contre la perte du broker.

### 1.2. Publier trois billets avec le contrat et la clé fournis

- **A2-1.2.1** Publier une seule fois le jeu. L'application valide le JSON avant l'envoi et attend l'ACK Kafka.


<!-- compose-pedagogie -->
**Avant de lancer :**

`run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`seed orders` construit trois billets avec `makeEvent`, les valide puis appelle `send` vers `dev.lab.design.v1`. `--solution` sélectionne [solutions/routing.ts](../solutions/routing.ts) pour la clé et [contracts/ticket-sold.v1.json](../contracts/ticket-sold.v1.json) pour le contrat ; aucun exercice à compléter.

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


```sh
docker compose run --rm app seed orders --solution --topic dev.lab.design.v1
```

- **A2-1.2.2** Vérifier `SEEDED orders`. Convention fournie : `ticketId` est le billet et la clé ; `eventId` est le concert ; `event_id` est l'identité du fait. Pourquoi ne pas choisir le concert comme clé ? **Réponse :** toutes les ventes d'un concert populaire iraient vers la même partition. Le code est déjà fourni dans `solutions/routing.ts` et `contracts/ticket-sold.v1.json`.

## 2. Observer une lecture partielle

### 2.1. Lire un record avec design-A2 puis afficher la supervision

- **A2-2.1.1** Lire un billet puis examiner les positions du groupe.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.design.v1` avec le groupe `design-A2`, afficher les records et valider leur offset suivant. `--count 1` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

Cette lecture change les positions du groupe, pas la base SQLite. Un groupe déjà utilisé reprend sa position ; `fromBeginning: true` ne force pas le retour au début s’il possède un offset valide.
**Code à lire — [src/bus.ts](../src/bus.ts#L67)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:67-68 -->
```ts
const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
  fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`supervise` appelle [supervise](../src/supervision.ts) pour `dev.lab.design.v1` et le groupe `design-A2`. L’Admin API lit l’offset bas conservé, la fin du journal et les positions validées. Cette commande ne consomme pas les messages, ne valide aucun nouvel offset et ne crée aucun PDF.

Dans le tableau affiché, le lag est log_end moins committed_next ; la position -1 est rendue par « offset absent » au lieu de produire un faux retard numérique.
**Code à lire — [src/supervision.ts](../src/supervision.ts#L6)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/supervision.ts:6-8 -->
```ts
const ends = await admin.fetchTopicOffsets(topic);
const committed = await admin.fetchOffsets({ groupId: group, topics: [topic] });
const positions = committed.find(t => t.topic === topic)?.partitions ?? [];
```


```sh
docker compose run --rm app read --topic dev.lab.design.v1 --group design-A2 --count 1
docker compose run --rm app supervise --topic dev.lab.design.v1 --group design-A2
```

- **A2-2.1.2** Vérifier `READ records=1`, la clé du billet et les données du concert. Le groupe n'a pas encore tout lu. `offset absent` indique une partition sans position validée ; ce n'est pas un lag nul. La distribution exacte de trois clés n'est pas un critère de réussite.

### 2.2. Lire les deux records restants avec le même groupe

- **A2-2.2.1** Terminer la lecture sans republier.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.design.v1` avec le groupe `design-A2`, afficher les records et valider leur offset suivant. `--count 2` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.
**Code à lire — [src/bus.ts](../src/bus.ts#L92)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:92-93 -->
```ts
await c.commitOffsets([{ topic: payload.topic, partition: payload.partition,
  offset: (BigInt(payload.message.offset) + 1n).toString() }]);
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`supervise` appelle [supervise](../src/supervision.ts) pour `dev.lab.design.v1` et le groupe `design-A2`. L’Admin API lit l’offset bas conservé, la fin du journal et les positions validées. Cette commande ne consomme pas les messages, ne valide aucun nouvel offset et ne crée aucun PDF.

Dans le tableau affiché, le lag est log_end moins committed_next ; la position -1 est rendue par « offset absent » au lieu de produire un faux retard numérique.

```sh
docker compose run --rm app read --topic dev.lab.design.v1 --group design-A2 --count 2
docker compose run --rm app supervise --topic dev.lab.design.v1 --group design-A2
```

- **A2-2.2.2** Vérifier `READ records=2`, soit trois records au total. Les partitions alimentées et validées ont un lag nul ; une partition vide peut garder `offset absent`. Le lag mesure des positions, pas le délai métier ni la production d'un PDF.

## 3. Contrôler le résultat

### 3.1. Comparer le topic, les records et les positions aux résultats fournis

- **A2-3.1.1** Contrôler trois partitions, trois billets lus en deux commandes, une clé par billet et une progression indépendante du groupe. L'ordre est garanti dans une partition ; ajouter des partitions peut modifier l'affectation future des clés.
- **A2-3.1.2** Si la lecture reste vide, vérifier le groupe et la publication unique. Si le nom du topic est incorrect, le reprendre à l'identique dans toutes les commandes. Conserver les données et les positions pour l'[atelier 6](Atelier06_Responsables-application.md), réalisé après l'atelier 4.
