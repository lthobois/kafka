# Atelier 8 — Mesurer la charge et observer une panne

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Objectif : distinguer débit produit, retard de lecture et disponibilité répliquée. Prérequis : README terminé, accès Docker sur la VM Linux, trois terminaux SSH et VM de 8 Go de RAM au total. Versions : Kafka 4.1.2 et le client TypeScript de la journée. Le cluster `kafka-ha` est indépendant du broker principal. Arrêter les conteneurs Streams/Connect avant cet atelier. Livrables observables : offsets du pic, ISR et nouveau leader. Les réponses de référence figurent après les actions ; aucun fichier de notes supplémentaire n'est requis.

Les références **A8-thématique.action.tâche** identifient les tâches. Toutes les commandes partent de `~/kafka`. Réaliser l'expérience sur des topics neufs ; les volumes persistent.

## 1. Observer un pic sur le broker principal

### 1.1. Créer dev.lab.peak.v1 avec trois partitions puis publier 1 000 ventes

- **A8-1.1.1** Créer le topic isolé et publier une seule fois le lot. Le producer valide les événements et envoie des lots de 100. Le temps affiché mesure cette publication, sans promettre le débit de production.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dev.lab.peak.v1 --partitions 3 --replication-factor 1
docker compose run --rm app seed peak --count 1000
```

- **A8-1.1.2** Vérifier `PEAK records=1000`, décrire le topic et lire trois ventes avec les commandes ci-dessous. Attendu : trois partitions ; aucune distribution exacte n'est imposée. Chaque billet a une clé différente ; `data.eventId` reste identique. Référence : choisir le concert comme clé concentrerait les ventes de ce concert.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.peak.v1
docker compose run --rm app read --topic dev.lab.peak.v1 --group sample-A8 --count 3
```

### 1.2. Ralentir un lecteur PDF et suivre les offsets du groupe peak-pdf-A8

- **A8-1.2.1** Dans un deuxième terminal, démarrer ce lecteur avec une pause après chaque lecture. Dans le premier, lancer ensuite la supervision plusieurs fois pendant son activité.

```sh
docker compose run --rm app read --topic dev.lab.peak.v1 --group peak-pdf-A8 --count 30 --delay 1 --timeout 60
```

```sh
docker compose run --rm app supervise --topic dev.lab.peak.v1 --group peak-pdf-A8
```

- **A8-1.2.2** Comparer offset de fin et offset validé, par partition. Attendu : un arriéré reste après 30 records ; une position absente doit être distinguée d'un zéro. Référence : lag = fin moins prochain offset validé lorsque ces deux valeurs existent. Ajouter des lecteurs du même groupe répartit les partitions ; un autre groupe crée un abonnement indépendant. L'expérience n'est pas un benchmark de PDF ni de 300 000 clients.

## 2. Observer une réplication sur trois brokers

### 2.1. Démarrer le projet kafka-ha et créer un topic RF3 avec minISR2

- **A8-2.1.1** Démarrer les trois nœuds KRaft combinés de laboratoire, puis créer le topic avec répartition explicite. Ce mode combiné économise les ressources du lab ; en production, dimensionner et isoler les rôles.

```sh
docker compose -p kafka-ha -f compose.ha.yaml up -d --wait --wait-timeout 240
docker compose -p kafka-ha -f compose.ha.yaml exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic dev.lab.ha.v1 --replica-assignment 1:2:3,2:3:1,3:1:2 --config min.insync.replicas=2
docker compose -p kafka-ha -f compose.ha.yaml exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic dev.lab.ha.v1
```

- **A8-2.1.2** Vérifier trois partitions, trois réplicas et trois ISR pour chacune. Les leaders initiaux attendus sont 1, 2 et 3. Attendre la convergence avant la panne. Si l'ISR n'est pas complet, examiner `logs --tail 100` et les ressources de la VM ; ne pas conclure à une tolérance aux pannes sur un cluster déjà dégradé.

### 2.2. Arrêter kafka1 pendant une publication puis vérifier la nouvelle élection

- **A8-2.2.1** Dans un deuxième terminal, publier le lot de charge ci-dessous, puis dans le premier terminal arrêter `kafka1`. Les records de ce test sont des octets synthétiques ; ils ne passent pas dans le worker métier JSON.

```sh
docker compose -p kafka-ha -f compose.ha.yaml exec kafka2 /opt/kafka/bin/kafka-producer-perf-test.sh --topic dev.lab.ha.v1 --num-records 3000 --record-size 100 --throughput 100 --producer-props bootstrap.servers=kafka2:9092,kafka3:9092 acks=all delivery.timeout.ms=120000
```

```sh
docker compose -p kafka-ha -f compose.ha.yaml stop kafka1
docker compose -p kafka-ha -f compose.ha.yaml exec kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka2:9092 --describe --topic dev.lab.ha.v1
```

- **A8-2.2.2** Attendre un leader vivant pour chaque partition, notamment P0. Référence : deux ISR restent après convergence et le quorum de contrôleurs garde deux membres sur trois. Le producer peut signaler des retries et une hausse de latence pendant l'élection ; il doit finir sans erreur finale si les délais et ressources suffisent. Sinon diagnostiquer au lieu d'affirmer une continuité démontrée. Ne pas arrêter un deuxième nœud : cela ferait aussi perdre le quorum KRaft, empêchant d'isoler expérimentalement l'effet de `min.insync.replicas`.

### 2.3. Redémarrer kafka1 et contrôler le retour des trois ISR

- **A8-2.3.1** Redémarrer puis décrire jusqu'au retour de trois ISR par partition. Le leader initial ne revient pas forcément immédiatement sur kafka1.

```sh
docker compose -p kafka-ha -f compose.ha.yaml up -d --wait --wait-timeout 240 kafka1
docker compose -p kafka-ha -f compose.ha.yaml exec kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka2:9092 --describe --topic dev.lab.ha.v1
```

- **A8-2.3.2** Arrêter le projet après vérification avec `docker compose -p kafka-ha -f compose.ha.yaml down`. Les volumes sont conservés. Validation finale : distinguer perte de broker, perte d'ISR et perte de quorum de contrôleurs ; ne pas déduire un SLA de cette seule expérience.
