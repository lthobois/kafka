# Transposer les acquis vers Aiven

La plateforme cible du client est **Aiven for Apache Kafka**, hébergée sur Azure selon le contexte fourni. Les ateliers obligatoires restent locaux ; aucune création de service payant ni accès au tenant réel n'est nécessaire.

## Du laboratoire au service managé

| Besoin | Laboratoire | Transposition à préparer avec la plateforme |
|---|---|---|
| Connexion | `kafka:9092`, réseau Docker local | URI de service, réseau, identité et TLS |
| Topic | CLI locale | Console ou API Aiven autorisée ; paramètres exposés et droits à vérifier |
| Abonnement | Client Confluent, `groupId`, `subscribe` | Même logique Kafka ; droits sur les topics et groupes |
| Diagnostic | Offsets et état SQLite | Groupes, journaux applicatifs, résultat métier et télémétrie |
| Reprise | Nouveau groupe ou plan de reset local | Procédure client, groupe inactif si reset, effets et périmètre contrôlés |

La documentation Aiven propose des méthodes SASL et certificat client ; les fichiers requis dépendent de la méthode choisie. Son exemple Node utilise `node-rdkafka`, qui repose comme le client du laboratoire sur librdkafka. Le laboratoire emploie `@confluentinc/kafka-javascript` : `ssl: true` et `sasl` dans le bloc `kafkaJS`, fichiers CA, certificat et clé en propriétés librdkafka (`ssl.ca.location`, `ssl.certificate.location`, `ssl.key.location`). [Connexion Node.js Aiven](https://aiven.io/docs/products/kafka/howto/connect-with-nodejs), [Client JavaScript Confluent](https://docs.confluent.io/kafka-clients/javascript/current/overview.html).

## Connexion facultative sur un service de formation existant

Préparation à faire avec l'équipe plateforme : service de formation accessible depuis Docker, topics dédiés, compte à droits minimaux, méthode d'authentification confirmée, CA et éventuels certificats utilisateur. Ne pas utiliser les topics métier de production pour cette recette. Le profil n'ouvre aucun accès réseau ni droit par lui-même.

1. Copier `.env.aiven.example` vers `.env.aiven` (`cp .env.aiven.example .env.aiven`, puis `nano .env.aiven`) ; remplir URI/port exacts et méthode.
2. Créer `certs` localement et y déposer la CA. Pour mTLS, ajouter certificat et clé utilisateur ; pour SASL, renseigner les identifiants de formation.
3. Garder ces fichiers exclus de Git et du contexte de construction Docker, comme prévu par les deux fichiers ignore.
4. Choisir un topic de formation déjà créé et un groupe convenu, puis lancer les commandes en remplaçant les noms d'exemple.

```sh
docker compose -f compose.aiven.yaml build app
docker compose -f compose.aiven.yaml run --rm app supervise --topic formation.shop.order-created.v1 --group formation-audit
```

Résultat attendu : lecture des positions si les droits et le réseau le permettent. Vérification : nom de service, topic et erreurs éventuelles. Ne pas lancer `init` ou `integration` sur le service client : ces commandes créent plusieurs topics de laboratoire. Les données de connexion du profil ne sont pas fournies dans le dépôt ; cette liaison n'a pas été testée sur le tenant client.

Le profil accepte `KAFKA_AUTH=mtls` ou SASL/SCRAM-SHA-256 avec TLS ; il ne couvre pas tous les mécanismes possibles. Ne jamais désactiver la vérification du certificat pour résoudre un problème de connexion.

## Trois niveaux de supervision

- Service : santé et capacité du service managé, selon les fonctions réellement activées.
- Transport : progression des groupes, lag et erreurs de connexion.
- Fonctionnel : commandes intégrées, expéditions produites, rejets, âge des reprises et délai bout en bout.

Aiven documente une télémétrie des groupes et des intégrations de supervision. Les indicateurs disponibles ne remplacent pas les contrôles métier. Les fonctions et permissions doivent être confirmées sur le service cible. [Groupes et télémétrie Aiven](https://aiven.io/docs/products/kafka/concepts/monitor-consumer-group).

Pour le reset, la documentation Aiven exige un groupe inactif et décrit l'utilisation de l'outil Kafka avec les paramètres d'authentification appropriés. Le laboratoire montre un `--dry-run` local ; ce n'est pas une autorisation de reset du groupe client. [Offsets chez Aiven](https://aiven.io/docs/products/kafka/howto/viewing-resetting-offset).

## OpenTelemetry exécutable, en extension

`src/telemetry.ts` instrumente explicitement publication et traitement, propage `traceparent` dans les headers et n'enregistre pas les payloads. L'export est désactivé par défaut. Le mode console fonctionne sans backend ; les commandes suivantes sont à utiliser dans une copie de démonstration pour ne pas changer les totaux des ateliers.

```sh
docker compose run --rm -e OTEL_TRACES_EXPORTER=console app seed orders --solution
docker compose run --rm -e OTEL_TRACES_EXPORTER=console app read --topic dev.shop.order-created.v1 --group trace-demo --count 3
```

Résultat attendu : spans `kafka.publish` et `kafka.process` avec identifiants de trace corrélés par record. Vérification : comparer les `traceId` entre émission et lecture. Les attributs ne contiennent ni fiche client ni corps de message. Les erreurs gérées et envoyées en DLQ restent à suivre dans les métriques applicatives ; un span sans erreur n'est pas une expédition réussie.

Pour un endpoint OTLP/HTTP de recette déjà accessible, l'application accepte les variables d'environnement standard de l'exporteur. Remplacer l'URL ci-dessous et passer les éventuels headers d'authentification par la gestion de secrets locale, sans les committer.

```sh
docker compose run --rm -e OTEL_TRACES_EXPORTER=otlp -e OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://collecteur.example/v1/traces app read --topic dev.shop.order-created.v1 --group trace-otlp-demo --count 3
```

Les backend Azure Monitor/Application Insights et Elastic cités dans la réunion nécessitent une chaîne d'export adaptée et validée avec les équipes. Un endpoint OTLP générique n'est pas à supposer disponible pour tout service Azure. L'instrumentation ne promet pas des graphes automatiques sans configuration ni des métriques Kafka complètes. [Exporteurs OpenTelemetry JavaScript](https://opentelemetry.io/docs/languages/js/exporters/).

## Outillage de consultation à évaluer

| Outil | Usage | Point à préparer |
|---|---|---|
| Console Aiven | Vue de service et fonctions disponibles dans le tenant | Rôles, droits et plan réels |
| CLI et scripts du laboratoire | Lecture ciblée, groupes et assertions reproductibles | Identité TLS/SASL et namespace de formation |
| Kafbat UI auto-hébergé | Exploration des topics et messages dans une interface | Hébergement, authentification, rôles, secrets et recette Aiven |
| Backend d'observabilité existant | Historique, alertes, corrélation applicative | Instrumentation, export, stockage et règles d'alerte |

Kafbat UI est une piste d'outil à recetter, pas un composant obligatoire ni une connexion déjà installée. Son interface ne transforme pas Kafka en table SQL modifiable record par record. [Projet et installation Kafbat UI](https://github.com/kafbat/kafka-ui).
