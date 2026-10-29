# Atelier 8 — Mesurer la charge et observer une panne

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Objectif : distinguer débit produit, retard de lecture et disponibilité répliquée. Prérequis : README terminé, accès Docker sur la VM Linux, trois terminaux SSH et VM de 8 Go de RAM au total. Versions : Kafka 4.1.2 et le client TypeScript de la journée. Le cluster `kafka-ha` est indépendant du broker principal. Arrêter les conteneurs Streams/Connect avant cet atelier. Livrables observables : offsets du pic, ISR et nouveau leader. Les réponses de référence figurent après les actions ; aucun fichier de notes supplémentaire n'est requis.

Les références **A8-thématique.action.tâche** identifient les tâches. Toutes les commandes partent de `~/kafka`. Réaliser l'expérience sur des topics neufs ; les volumes persistent.

## 1. Observer un pic sur le broker principal

### 1.1. Créer dev.lab.peak.v1 avec trois partitions puis publier 1 000 ventes

- **A8-1.1.1** Créer le topic isolé et publier une seule fois le lot. Le producer valide les événements et envoie des lots de 100. Le temps affiché mesure cette publication, sans promettre le débit de production.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.peak.v1`. `--partitions 3` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** `run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`seed peak --count 1000` fabrique ce nombre de ventes, attribue des identités propres à cette exécution et envoie des lots de 100 dans `dev.lab.peak.v1`. La mesure affichée couvre cette publication synthétique, pas des achats HTTP réels.
**Code à lire — [src/cli.ts](../src/cli.ts#L76)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:76-83 -->
```ts
const messages = Array.from({ length: Math.min(100, count - start + 1) }, (_, i) => {
  const event = makeEvent(start + i);
  event.event_id = `${run}-${start + i}`;
  event.data.ticketId = `${run}-TKT-${start + i}`;
  event.occurred_at = new Date().toISOString();
  validate(event);
  return { key: event.data.ticketId, value: JSON.stringify(event) };
});
```


```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.peak.v1 --partitions 3 --replication-factor 1
docker compose run --rm app seed peak --count 1000
```

- **A8-1.1.2** Vérifier `PEAK records=1000`, décrire le topic et lire trois ventes avec les commandes ci-dessous. Attendu : trois partitions ; aucune distribution exacte n'est imposée. Chaque billet a une clé différente ; `data.eventId` reste identique. Référence : choisir le concert comme clé concentrerait les ventes de ce concert.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.peak.v1`. 

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.peak.v1` avec le groupe `sample-A8`, afficher les records et valider leur offset suivant. `--count 3` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

Cette lecture change les positions du groupe, pas la base SQLite. Un groupe déjà utilisé reprend sa position ; `fromBeginning: true` ne force pas le retour au début s’il possède un offset valide.
**Code à lire — [src/bus.ts](../src/bus.ts#L67)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:67-68 -->
```ts
const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
  fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
```


```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.peak.v1
docker compose run --rm app read --topic dev.lab.peak.v1 --group sample-A8 --count 3
```

### 1.2. Ralentir un lecteur PDF et suivre les offsets du groupe peak-pdf-A8

- **A8-1.2.1** Dans un deuxième terminal, démarrer ce lecteur avec une pause après chaque lecture. Dans le premier, lancer ensuite la supervision plusieurs fois pendant son activité.


<!-- compose-pedagogie -->
**Avant de lancer :**

Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.peak.v1` avec le groupe `peak-pdf-A8`, afficher les records et valider leur offset suivant. `--count 30` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 60` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

`--delay 1` ajoute cette pause en secondes après chaque lecture ; ce ralentissement est dans le script, pas dans Kafka.
**Code à lire — [src/bus.ts](../src/bus.ts#L92)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:92-93 -->
```ts
await c.commitOffsets([{ topic: payload.topic, partition: payload.partition,
  offset: (BigInt(payload.message.offset) + 1n).toString() }]);
```


```sh
docker compose run --rm app read --topic dev.lab.peak.v1 --group peak-pdf-A8 --count 30 --delay 1 --timeout 60
```


<!-- compose-pedagogie -->
**Avant de lancer :**

Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`supervise` appelle [supervise](../src/supervision.ts) pour `dev.lab.peak.v1` et le groupe `peak-pdf-A8`. L’Admin API lit l’offset bas conservé, la fin du journal et les positions validées. Cette commande ne consomme pas les messages, ne valide aucun nouvel offset et ne crée aucun PDF.

Dans le tableau affiché, le lag est log_end moins committed_next ; la position -1 est rendue par « offset absent » au lieu de produire un faux retard numérique.
**Code à lire — [src/supervision.ts](../src/supervision.ts#L6)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/supervision.ts:6-8 -->
```ts
const ends = await admin.fetchTopicOffsets(topic);
const committed = await admin.fetchOffsets({ groupId: group, topics: [topic] });
const positions = committed.find(t => t.topic === topic)?.partitions ?? [];
```


```sh
docker compose run --rm app supervise --topic dev.lab.peak.v1 --group peak-pdf-A8
```

- **A8-1.2.2** Comparer offset de fin et offset validé, par partition. Attendu : un arriéré reste après 30 records ; une position absente doit être distinguée d'un zéro. Référence : lag = fin moins prochain offset validé lorsque ces deux valeurs existent. Ajouter des lecteurs du même groupe répartit les partitions ; un autre groupe crée un abonnement indépendant. L'expérience n'est pas un benchmark de PDF ni de 300 000 clients.

## 2. Observer une réplication sur trois brokers

### 2.1. Démarrer le projet kafka-ha et créer un topic RF3 avec minISR2

- **A8-2.1.1** Démarrer les trois nœuds KRaft combinés de laboratoire, puis créer le topic avec répartition explicite. Ce mode combiné économise les ressources du lab ; en production, dimensionner et isoler les rôles.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. `--wait` attend leur état running ou healthy selon le service ; `--wait-timeout 240` borne cette attente en secondes.

Sans nom de service final, cette configuration démarre les trois nœuds kafka1/kafka2/kafka3. Avec kafka1 en dernier argument, elle redémarre ce seul nœud. La topologie et les volumes proviennent de [compose.ha.yaml](../compose.ha.yaml). Aucun worker métier n’est lancé.

**Commande 2 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`exec kafka1` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka1:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.ha.v1`. 

`--replica-assignment 1:2:3,2:3:1,3:1:2` décrit les trois listes de réplicas ; leur premier élément est le leader préféré. `min.insync.replicas=2` fixe le seuil utilisé avec acks=all.

Configuration du serveur à relier à cet outil : [compose.ha.yaml](../compose.ha.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 3 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`exec kafka1` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka1:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.ha.v1`. 

Configuration du serveur à relier à cet outil : [compose.ha.yaml](../compose.ha.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

```sh
docker compose -p kafka-ha -f compose.ha.yaml up -d --wait --wait-timeout 240
docker compose -p kafka-ha -f compose.ha.yaml exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic dev.lab.ha.v1 --replica-assignment 1:2:3,2:3:1,3:1:2 --config min.insync.replicas=2
docker compose -p kafka-ha -f compose.ha.yaml exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic dev.lab.ha.v1
```

- **A8-2.1.2** Vérifier trois partitions, trois réplicas et trois ISR pour chacune. Les leaders initiaux attendus sont 1, 2 et 3. Attendre la convergence avant la panne. Si l'ISR n'est pas complet, examiner `logs --tail 100` et les ressources de la VM ; ne pas conclure à une tolérance aux pannes sur un cluster déjà dégradé.

### 2.2. Arrêter kafka1 pendant une publication puis vérifier la nouvelle élection

- **A8-2.2.1** Dans un deuxième terminal, publier le lot de charge ci-dessous, puis dans le premier terminal arrêter `kafka1`. Les records de ce test sont des octets synthétiques ; ils ne passent pas dans le worker métier JSON.


<!-- compose-pedagogie -->
**Avant de lancer :**

Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`exec kafka2` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

`kafka-producer-perf-test.sh` lance le producteur de charge officiel : 3000 records de 100 octets, débit cible 100/s. acks=all et delivery.timeout.ms concernent ses envois. Le contenu est synthétique, sans contrat ticket.sold ni génération PDF ; ce n’est pas src/cli.ts seed peak.

```sh
docker compose -p kafka-ha -f compose.ha.yaml exec kafka2 /opt/kafka/bin/kafka-producer-perf-test.sh --topic dev.lab.ha.v1 --num-records 3000 --record-size 100 --throughput 100 --producer-props bootstrap.servers=kafka2:9092,kafka3:9092 acks=all delivery.timeout.ms=120000
```


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`stop` arrête les services nommés en fin de commande, sans supprimer leurs conteneurs ni leurs volumes. Arrêter kafka1 provoque la panne observée ; arrêter streams/connect interrompt ces applications, pas le broker principal.

**Commande 2 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`exec kafka2` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka2:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.ha.v1`. 

Configuration du serveur à relier à cet outil : [compose.ha.yaml](../compose.ha.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

```sh
docker compose -p kafka-ha -f compose.ha.yaml stop kafka1
docker compose -p kafka-ha -f compose.ha.yaml exec kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka2:9092 --describe --topic dev.lab.ha.v1
```

- **A8-2.2.2** Attendre un leader vivant pour chaque partition, notamment P0. Référence : deux ISR restent après convergence et le quorum de contrôleurs garde deux membres sur trois. Le producer peut signaler des retries et une hausse de latence pendant l'élection ; il doit finir sans erreur finale si les délais et ressources suffisent. Sinon diagnostiquer au lieu d'affirmer une continuité démontrée. Ne pas arrêter un deuxième nœud : cela ferait aussi perdre le quorum KRaft, empêchant d'isoler expérimentalement l'effet de `min.insync.replicas`.

### 2.3. Redémarrer kafka1 et contrôler le retour des trois ISR

- **A8-2.3.1** Redémarrer puis décrire jusqu'au retour de trois ISR par partition. Le leader initial ne revient pas forcément immédiatement sur kafka1.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. `--wait` attend leur état running ou healthy selon le service ; `--wait-timeout 240` borne cette attente en secondes.

Sans nom de service final, cette configuration démarre les trois nœuds kafka1/kafka2/kafka3. Avec kafka1 en dernier argument, elle redémarre ce seul nœud. La topologie et les volumes proviennent de [compose.ha.yaml](../compose.ha.yaml). Aucun worker métier n’est lancé.

**Commande 2 du bloc.** Configuration : [compose.ha.yaml](../compose.ha.yaml). Les fichiers sont combinés dans cet ordre.

Le projet `kafka-ha` possède ses propres conteneurs, réseau et volumes nommés ; il reste distinct du broker principal.

`exec kafka2` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` décrit le topic et ses partitions. `--bootstrap-server kafka2:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.ha.v1`. 

Configuration du serveur à relier à cet outil : [compose.ha.yaml](../compose.ha.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

```sh
docker compose -p kafka-ha -f compose.ha.yaml up -d --wait --wait-timeout 240 kafka1
docker compose -p kafka-ha -f compose.ha.yaml exec kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka2:9092 --describe --topic dev.lab.ha.v1
```

- **A8-2.3.2** Arrêter le projet après vérification avec `docker compose -p kafka-ha -f compose.ha.yaml down`. Les volumes sont conservés. Validation finale : distinguer perte de broker, perte d'ISR et perte de quorum de contrôleurs ; ne pas déduire un SLA de cette seule expérience.
