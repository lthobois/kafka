# Atelier 9 — Exécuter Streams et Connect

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Objectif : observer une application Kafka Streams et un connecteur source réel. Prérequis : broker principal sain, Docker Compose, accès aux registres Docker et à Maven Central pendant la préparation, 4 Gio de RAM supplémentaires. Le projet TypeScript reste le socle ; Java 21 et Maven sont confinés à une image d'extension car Streams est une bibliothèque Java. Le Dockerfile fige Kafka Streams et Connect File à 4.1.2. Le connecteur fichier enseigne le framework sans installer une base CDC. Livrables : comptages par concert et lignes acheminées par Connect. Les références **A9-thématique.action.tâche** identifient les tâches ; contrôle autonome dans les outils.

## 1. Calculer les ventes par concert

### 1.1. Construire l'image d'extension et préparer les topics Streams

- **A9-1.1.1** Depuis `~/kafka`, préparer l'image et les topics dédiés. Le téléchargement d'outils passe par le Dockerfile ; sources : [Kafka Streams](https://kafka.apache.org/41/streams/), artefact Maven `org.apache.kafka:kafka-streams:4.1.2`. L'application principale ne change pas de client.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`--profile extensions` rend disponibles les services de ce profil.

`build` construit les images ; il ne démarre pas encore les applications. [extensions/Dockerfile](../extensions/Dockerfile) compile le Java avec Maven, copie les dépendances dans l’image Kafka et installe la bibliothèque native nécessaire à RocksDB.
**Code à lire — [extensions/Dockerfile](../extensions/Dockerfile#L5)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: extensions/Dockerfile:5-5 -->
```dockerfile
RUN mvn -B package dependency:copy-dependencies -DoutputDirectory=/src/lib
```


**Commande 2 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.streams.sold.v1`. `--partitions 3` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 3 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.streams.statistics.v1`. `--partitions 3` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

```sh
docker compose -f compose.yaml -f compose.extensions.yaml --profile extensions build streams connect
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.streams.sold.v1 --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.streams.statistics.v1 --partitions 3 --replication-factor 1
```

- **A9-1.1.2** Lire `extensions/src/main/java/training/TicketStatistics.java`. Référence : `selectKey` choisit le concert, `groupByKey` implique un repartitionnement, `count` maintient un state store et `to` publie la sortie. Le code compte des records déjà validés, sans déduplication métier. Il utilise le timestamp Kafka et une fenêtre de dix secondes sans grâce ; un vrai calcul métier devrait choisir timestamp et gestion des retards.

### 1.2. Démarrer Streams puis publier trois billets et lire les compteurs

- **A9-1.2.1** Lancer le traitement puis publier une seule fois les trois événements de référence.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`--profile extensions` rend disponibles les services de ce profil.

`up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. Le retour de la commande ne prouve pas encore qu’une application a produit un résultat.

Le service streams exécute `training.TicketStatistics` en Java, suivant [compose.extensions.yaml](../compose.extensions.yaml). Il lit les ventes, choisit eventId, agrège par fenêtre et écrit le topic statistique. Ce n’est pas une branche du programme Node.js.
**Code à lire — [extensions/src/main/java/training/TicketStatistics.java](../extensions/src/main/java/training/TicketStatistics.java#L34)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: extensions/src/main/java/training/TicketStatistics.java:34-40 -->
```java
.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
.count(Materialized.as("sales-by-concert"))
.toStream()
.selectKey((window, count) -> window.key() + "@" + window.window().start())
.mapValues(count -> count.toString())
.to("dev.lab.streams.statistics.v1", Produced.with(Serdes.String(), Serdes.String()));
```


**Commande 2 du bloc.** `run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`seed orders` construit trois billets avec `makeEvent`, les valide puis appelle `send` vers `dev.lab.streams.sold.v1`. `--solution` sélectionne [solutions/routing.ts](../solutions/routing.ts) pour la clé et [contracts/ticket-sold.v1.json](../contracts/ticket-sold.v1.json) pour le contrat ; aucun exercice à compléter.

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


**Commande 3 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

`kafka-console-consumer.sh` est un lecteur texte officiel du topic `dev.lab.streams.statistics.v1`. `--from-beginning` demande le début pour une lecture sans position valide ; les options limitent la lecture par délai avec --timeout-ms, exprimé en millisecondes. Ce lecteur n’exécute ni readRecords ni le traitement SQLite.

`--isolation-level read_committed` masque les publications de transactions non validées ; `--property print.key=true` affiche également la clé.

```sh
docker compose -f compose.yaml -f compose.extensions.yaml --profile extensions up -d streams
docker compose run --rm app seed orders --solution --topic dev.lab.streams.sold.v1
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dev.lab.streams.statistics.v1 --from-beginning --isolation-level read_committed --timeout-ms 30000 --property print.key=true
```

- **A9-1.2.2** Vérifier des clés `CONCERT-2026@<début_fenêtre>` et des compteurs croissants pour chaque fenêtre. Attendu : somme des dernières valeurs par fenêtre = 3, si une seule publication a eu lieu. Les mises à jour intermédiaires peuvent être regroupées ; ne pas imposer trois lignes ni additionner toutes les mises à jour. Le consumer termine éventuellement avec un timeout normal après la période sans nouvelle sortie.

### 1.3. Examiner changelog et reprise du state store

- **A9-1.3.1** Lister les topics et retrouver les noms préfixés `ticket-statistics-lab-v1`, notamment `sales-by-concert-changelog` et le repartitionnement. Arrêter puis redémarrer l'application ; consulter ses logs. L'état local du conteneur n'est pas placé dans un volume et peut être restauré depuis Kafka lors d'une recréation.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` affiche la liste des topics. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`stop` arrête les services nommés en fin de commande, sans supprimer leurs conteneurs ni leurs volumes. Arrêter kafka1 provoque la panne observée ; arrêter streams/connect interrompt ces applications, pas le broker principal.

**Commande 3 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`--profile extensions` rend disponibles les services de ce profil.

`up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. Le retour de la commande ne prouve pas encore qu’une application a produit un résultat.

`--force-recreate` remplace le conteneur même si sa configuration n’a pas changé ; ses fichiers internes non montés sont perdus, ses volumes nommés restent conservés.

Le service streams exécute `training.TicketStatistics` en Java, suivant [compose.extensions.yaml](../compose.extensions.yaml). Il lit les ventes, choisit eventId, agrège par fenêtre et écrit le topic statistique. Ce n’est pas une branche du programme Node.js.

**Commande 4 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`logs --tail 100` affiche les dernières lignes déjà écrites par le service ; il ne redémarre ni ne rejoue rien. Pour Streams, les logs proviennent de TicketStatistics et des bibliothèques Kafka ; pour Connect, du worker et de ses plugins.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose -f compose.yaml -f compose.extensions.yaml stop streams
docker compose -f compose.yaml -f compose.extensions.yaml --profile extensions up -d --force-recreate streams
docker compose -f compose.yaml -f compose.extensions.yaml logs --tail 100 streams
```

- **A9-1.3.2** Comparer au code : le store conserve un compteur, jamais la disponibilité transactionnelle des places. Référence : exactement une fois dans le pipeline Kafka ne déduplique pas deux publications métier distinctes portant le même fait. Ne pas republier le lot pour tester une restauration à résultat constant.

## 2. Transporter des lignes avec Kafka Connect

### 2.1. Créer le topic puis démarrer FileStreamSourceConnector

- **A9-2.1.1** Préparer le topic et lancer Connect. Ce plugin est un exemple pédagogique, pas une solution CDC de production. Le worker standalone sauvegarde les positions du fichier dans un volume.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le script officiel `/opt/kafka/bin/kafka-topics.sh` crée le topic. `--bootstrap-server kafka:9092` donne l’adresse Kafka joignable depuis le conteneur. Il contacte Kafka directement : aucun passage par src/cli.ts, aucun billet publié et aucun effet SQLite.

Cible : `dev.lab.connect.sold.v1`. `--partitions 1` règle les partitions ; `--replication-factor 1` règle le nombre de copies de chaque partition.

Configuration du serveur à relier à cet outil : [compose.yaml](../compose.yaml). L’outil lui-même est fourni par l’image Kafka, pas par le code TypeScript du laboratoire.

**Commande 2 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`--profile extensions` rend disponibles les services de ce profil.

`up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. `--wait` attend leur état running ou healthy selon le service ; `--wait-timeout 180` borne cette attente en secondes.

Le service connect lance connect-standalone.sh avec [extensions/connect-worker.properties](../extensions/connect-worker.properties) et [extensions/connect-file.properties](../extensions/connect-file.properties). Son healthcheck attend l’état RUNNING de la tâche source ; le volume connect-data conserve le fichier et ses positions.
**Code à lire — [extensions/connect-file.properties](../extensions/connect-file.properties#L1)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: extensions/connect-file.properties:1-5 -->
```properties
name=ticket-file-source
connector.class=org.apache.kafka.connect.file.FileStreamSourceConnector
tasks.max=1
file=/lab/data/sales.jsonl
topic=dev.lab.connect.sold.v1
```


**Commande 3 du bloc.** Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`exec connect` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

Le shell `sh -c` du conteneur Connect exécute `printf` puis `>>` ajoute une ligne au fichier /lab/data/sales.jsonl. La commande ne publie pas elle-même dans Kafka : le connecteur détecte la nouvelle ligne et la transporte.

**Commande 4 du bloc.** `exec kafka` ajoute un processus dans le conteneur déjà démarré de ce service ; il ne crée pas le conteneur ponctuel `app`.

`kafka-console-consumer.sh` est un lecteur texte officiel du topic `dev.lab.connect.sold.v1`. `--from-beginning` demande le début pour une lecture sans position valide ; les options limitent la lecture par nombre avec --max-messages. Ce lecteur n’exécute ni readRecords ni le traitement SQLite.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.connect.sold.v1 --partitions 1 --replication-factor 1
docker compose -f compose.yaml -f compose.extensions.yaml --profile extensions up -d --wait --wait-timeout 180 connect
docker compose -f compose.yaml -f compose.extensions.yaml exec connect sh -c 'printf "%s\n" "{\"ticketId\":\"TKT-C1\",\"eventId\":\"CONCERT-2026\"}" >> /lab/data/sales.jsonl'
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dev.lab.connect.sold.v1 --from-beginning --max-messages 1 --timeout-ms 30000
```

- **A9-2.1.2** Vérifier que la ligne JSON du fichier devient une valeur Kafka. Examiner les logs et les deux fichiers `.properties`. Référence : framework Connect, plugin FileStreamSourceConnector, un fichier source, un topic cible et des positions persistées. Une ligne relue après incident peut être réémise ; le destinataire doit être idempotent.

### 2.2. Comparer le connecteur fichier au CDC d'une outbox

- **A9-2.2.1** Comparer la manipulation à la chaîne de référence : transaction SQL vente/outbox, journal SQL, connecteur Debezium source dans Connect, routage outbox, `ticket.sold`. Réponse : le fichier n'a ni transaction SQL ni journal de réplication ; cette manipulation ne démontre pas le CDC. Un déploiement Debezium demande base compatible, droits de réplication, rétention du journal, snapshots, plugin et configuration. Le [routeur outbox Debezium](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) décrit ce complément.
- **A9-2.2.2** Arrêter seulement les services d'extension. **Différence à retenir :** l'application Streams calcule les statistiques et écrit son résultat ; Connect exécute un plugin qui transporte ici les lignes d'un fichier. Retrouver `to` dans le code Java et `connector.class` dans les propriétés pour vérifier ces responsabilités.


<!-- compose-pedagogie -->
**Avant de lancer :**

Configuration : [compose.yaml](../compose.yaml), puis [compose.extensions.yaml](../compose.extensions.yaml). Les fichiers sont combinés dans cet ordre.

`stop` arrête les services nommés en fin de commande, sans supprimer leurs conteneurs ni leurs volumes. Arrêter kafka1 provoque la panne observée ; arrêter streams/connect interrompt ces applications, pas le broker principal.

```sh
docker compose -f compose.yaml -f compose.extensions.yaml stop streams connect
```

Si Maven ou un registre est inaccessible, utiliser une image préparée sur la VM ; sinon lire le code et le corrigé comme repli conceptuel. Ce repli ne prouve pas une exécution. Si aucun compteur n'arrive, vérifier les logs, les trois topics internes et la présence des ventes avant de redémarrer. Ne pas changer `application.id` pour masquer une erreur : cela crée un autre groupe et un nouvel état.
