# Atelier 2 — Publier des ventes et observer leur progression

Objectif : créer un topic à trois partitions, publier trois billets avec le contrat fourni puis observer les offsets d'un groupe. Le topic `dev.lab.design.v1` isole cette expérience. Le fait publié reste `ticket.sold` ; le nom de production de référence est `dev.ticketing.ticket.sold.v1`.

Prérequis : [README](../README.md) terminé ; l'atelier 1 n'est pas nécessaire pour les commandes. Environnement : même VM Linux, Docker, Kafka 4.1.2 et client Node.js fournis. Un terminal suffit. Aucun fichier ni code à rédiger ; `--solution` sélectionne le contrat et la clé prêts à l'emploi. Les références **A2-thématique.action.tâche** identifient les tâches.

## 1. Créer le journal des ventes

### 1.1. Créer dev.lab.design.v1 avec trois partitions et sept jours de rétention

- **A2-1.1.1** Créer le topic puis afficher sa configuration. Les trois partitions permettent de distribuer les clés ; RF1 est une simplification du laboratoire.

```sh
cd ~/kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.design.v1 --partitions 3 --replication-factor 1 --config retention.ms=604800000
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.design.v1
```

- **A2-1.1.2** Vérifier `PartitionCount: 3`, `ReplicationFactor: 1` et `retention.ms=604800000`. Sept jours est une valeur de laboratoire ; RF1 ne protège pas contre la perte du broker.

### 1.2. Publier trois billets avec le contrat et la clé fournis

- **A2-1.2.1** Publier une seule fois le jeu. L'application valide le JSON avant l'envoi et attend l'ACK Kafka.

```sh
docker compose run --rm app seed orders --solution --topic dev.lab.design.v1
```

- **A2-1.2.2** Vérifier `SEEDED orders`. Convention fournie : `ticketId` est le billet et la clé ; `eventId` est le concert ; `event_id` est l'identité du fait. Pourquoi ne pas choisir le concert comme clé ? **Réponse :** toutes les ventes d'un concert populaire iraient vers la même partition. Le code est déjà fourni dans `solutions/routing.ts` et `contracts/ticket-sold.v1.json`.

## 2. Observer une lecture partielle

### 2.1. Lire un record avec design-A2 puis afficher la supervision

- **A2-2.1.1** Lire un billet puis examiner les positions du groupe.

```sh
docker compose run --rm app read --topic dev.lab.design.v1 --group design-A2 --count 1
docker compose run --rm app supervise --topic dev.lab.design.v1 --group design-A2
```

- **A2-2.1.2** Vérifier `READ records=1`, la clé du billet et les données du concert. Le groupe n'a pas encore tout lu. `offset absent` indique une partition sans position validée ; ce n'est pas un lag nul. La distribution exacte de trois clés n'est pas un critère de réussite.

### 2.2. Lire les deux records restants avec le même groupe

- **A2-2.2.1** Terminer la lecture sans republier.

```sh
docker compose run --rm app read --topic dev.lab.design.v1 --group design-A2 --count 2
docker compose run --rm app supervise --topic dev.lab.design.v1 --group design-A2
```

- **A2-2.2.2** Vérifier `READ records=2`, soit trois records au total. Les partitions alimentées et validées ont un lag nul ; une partition vide peut garder `offset absent`. Le lag mesure des positions, pas le délai métier ni la production d'un PDF.

## 3. Contrôler le résultat

### 3.1. Comparer le topic, les records et les positions aux résultats fournis

- **A2-3.1.1** Contrôler trois partitions, trois billets lus en deux commandes, une clé par billet et une progression indépendante du groupe. L'ordre est garanti dans une partition ; ajouter des partitions peut modifier l'affectation future des clés.
- **A2-3.1.2** Si la lecture reste vide, vérifier le groupe et la publication unique. Si le nom du topic est incorrect, le reprendre à l'identique dans toutes les commandes. Conserver les données et les positions pour l'[atelier 6](Atelier06_Responsables-application.md), réalisé après l'atelier 4.
