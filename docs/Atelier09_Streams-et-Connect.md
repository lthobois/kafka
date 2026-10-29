# Atelier 9 — Exécuter Streams et Connect

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Objectif : observer une application Kafka Streams et un connecteur source réel. Prérequis : broker principal sain, Docker Compose, accès aux registres Docker et à Maven Central pendant la préparation, 4 Gio de RAM supplémentaires. Le projet TypeScript reste le socle ; Java 21 et Maven sont confinés à une image d'extension car Streams est une bibliothèque Java. Le Dockerfile fige Kafka Streams et Connect File à 4.1.2. Le connecteur fichier enseigne le framework sans installer une base CDC. Livrables : comptages par concert et lignes acheminées par Connect. Les références **A9-thématique.action.tâche** identifient les tâches ; contrôle autonome dans les outils.

## 1. Calculer les ventes par concert

### 1.1. Construire l'image d'extension et préparer les topics Streams

- **A9-1.1.1** Depuis `~/kafka`, préparer l'image et les topics dédiés. Le téléchargement d'outils passe par le Dockerfile ; sources : [Kafka Streams](https://kafka.apache.org/41/streams/), artefact Maven `org.apache.kafka:kafka-streams:4.1.2`. L'application principale ne change pas de client.

```sh
docker compose -f compose.yaml -f compose.extensions.yaml --profile extensions build streams connect
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.streams.sold.v1 --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.streams.statistics.v1 --partitions 3 --replication-factor 1
```

- **A9-1.1.2** Lire `extensions/src/main/java/training/TicketStatistics.java`. Référence : `selectKey` choisit le concert, `groupByKey` implique un repartitionnement, `count` maintient un state store et `to` publie la sortie. Le code compte des records déjà validés, sans déduplication métier. Il utilise le timestamp Kafka et une fenêtre de dix secondes sans grâce ; un vrai calcul métier devrait choisir timestamp et gestion des retards.

### 1.2. Démarrer Streams puis publier trois billets et lire les compteurs

- **A9-1.2.1** Lancer le traitement puis publier une seule fois les trois événements de référence.

```sh
docker compose -f compose.yaml -f compose.extensions.yaml --profile extensions up -d streams
docker compose run --rm app seed orders --solution --topic dev.lab.streams.sold.v1
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dev.lab.streams.statistics.v1 --from-beginning --isolation-level read_committed --timeout-ms 30000 --property print.key=true
```

- **A9-1.2.2** Vérifier des clés `CONCERT-2026@<début_fenêtre>` et des compteurs croissants pour chaque fenêtre. Attendu : somme des dernières valeurs par fenêtre = 3, si une seule publication a eu lieu. Les mises à jour intermédiaires peuvent être regroupées ; ne pas imposer trois lignes ni additionner toutes les mises à jour. Le consumer termine éventuellement avec un timeout normal après la période sans nouvelle sortie.

### 1.3. Examiner changelog et reprise du state store

- **A9-1.3.1** Lister les topics et retrouver les noms préfixés `ticket-statistics-lab-v1`, notamment `sales-by-concert-changelog` et le repartitionnement. Arrêter puis redémarrer l'application ; consulter ses logs. L'état local du conteneur n'est pas placé dans un volume et peut être restauré depuis Kafka lors d'une recréation.

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

```sh
docker compose -f compose.yaml -f compose.extensions.yaml stop streams connect
```

Si Maven ou un registre est inaccessible, utiliser une image préparée sur la VM ; sinon lire le code et le corrigé comme repli conceptuel. Ce repli ne prouve pas une exécution. Si aucun compteur n'arrive, vérifier les logs, les trois topics internes et la présence des ventes avant de redémarrer. Ne pas changer `application.id` pour masquer une erreur : cela crée un autre groupe et un nouvel état.
