# Atelier 5 — Configurer et superviser un topic

Vous préparez un flux de formation. L'objectif est de comprendre les paramètres que l'équipe applicative doit demander ou contrôler sur un service managé, sans installer ni administrer les brokers. Livrable : topic de laboratoire décrit et lag interprété.

Préparation : Compose démarré, Kafka 4.1.2 et outils inclus dans son image ; Node.js 24.14.0, client Confluent `@confluentinc/kafka-javascript` 1.11.0. Aucun compte Aiven ; les commandes ne concernent que le broker local. Un terminal suffit. Les contrôles sont dans les outils, aucun fichier de notes n'est requis.

## 1. Préparer le topic de laboratoire

### 1.1. Créer dev.lab.infra.v1 avec une partition et une rétention d'un jour

- **A5-1.1.1** Créer le topic. Résultat attendu : `Created topic dev.lab.infra.v1.` L'avertissement qui le précède sur les points et les tirets bas dans les noms de topics est normal : nos noms n'utilisent que des points. `--if-not-exists` permet de relancer sans recréer un topic existant ; il ne restaure pas ses paramètres.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dev.lab.infra.v1 --partitions 1 --replication-factor 1 --config retention.ms=86400000
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic dev.lab.infra.v1
```

- **A5-1.1.2** Vérifier une partition et un facteur de réplication de 1 dans l'environnement neuf. Interprétation : c'est une simplification locale ; une seule copie ne supporte pas la perte du broker. Sur Aiven, les choix de service et de réplication relèvent du service souscrit et de la plateforme, pas d'une installation par les développeurs.

### 1.2. Étendre à trois partitions et demander sept jours de rétention

- **A5-1.2.1** Modifier uniquement ce topic de laboratoire, puis le décrire.

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

```sh
docker compose run --rm app seed orders --solution --topic dev.lab.infra.v1
docker compose run --rm app read --topic dev.lab.infra.v1 --group supervision-A5 --count 1
docker compose run --rm app supervise --topic dev.lab.infra.v1 --group supervision-A5
```

- **A5-2.1.2** Vérifier que le groupe n'a pas tout lu. Certaines partitions peuvent afficher `offset absent` parce qu'aucun offset n'y a été committé. Un lag numérique compare l'offset de fin et le prochain offset validé dans une même partition. Ce nombre n'est ni un temps de retard ni une preuve que l'effet métier a réussi.

### 2.2. Terminer la lecture et vérifier que les offsets avancent

- **A5-2.2.1** Lire les deux records restants avec le même groupe, puis contrôler à nouveau.

```sh
docker compose run --rm app read --topic dev.lab.infra.v1 --group supervision-A5 --count 2
docker compose run --rm app supervise --topic dev.lab.infra.v1 --group supervision-A5
```

- **A5-2.2.2** Résultat attendu : trois records lus au total. Les partitions ayant reçu des records et désormais committées ont un lag nul ; une partition vide peut conserver `offset absent`. Vérifier les sorties. Le contrôle du topic et l'interprétation du lag constituent la preuve de réussite.

En cas de commandes relancées, les totaux changent ; un groupe neuf repart du début conservé. Sur Aiven, traduire ces contrôles dans la console ou les outils autorisés, avec les droits disponibles.
