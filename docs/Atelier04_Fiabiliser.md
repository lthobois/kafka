# Atelier 4 — Fiabiliser un traitement applicatif

La préparation logistique ne doit pas être exécutée deux fois pour le même fait. Vous comparez un traitement naïf et un traitement idempotent, puis simulez des erreurs et un arrêt avant commit. Livrables : états SQLite observés, DLQ inspectée et décision de reprise.

Préparation : README exécuté, Kafka 4.1.2, Node.js 24.14.0 et client Confluent `@confluentinc/kafka-javascript` 1.11.0 en conteneurs Linux. SQLite est fourni par Node. Aucun compte cloud. Les noms de topics du laboratoire isolent cet atelier des publications de l'atelier 2. Exécuter chaque publication une seule fois dans un environnement neuf. Aucun changement de code n'est obligatoire pour le parcours responsable d'application.

Les références en gras identifient chaque tâche. Créer un seul fichier texte local `reponses-A4.txt` lorsque demandé ; aucune modification de cette page Git.

## 1. Mesurer l'effet des doublons

### 1.1. Publier cinq records de résilience et exécuter le traitement naïf

- **A4-1.1.1** Ouvrir `src/cli.ts` à la branche `resilience`. Repérer les cinq records : un fait valide, son doublon, un fait avec deux échecs transitoires, une quantité négative, un fait avec une panne persistante. Les en-têtes `lab-failures` servent uniquement à injecter les pannes du laboratoire.
- **A4-1.1.2** Publier le jeu puis lancer le worker naïf. Résultat attendu : cinq records consommés, trois effets acceptés, aucun doublon détecté, quatre retries et deux DLQ.

```sh
docker compose run --rm app seed resilience --topic dev.lab.resilience.v1
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group naive-A4 --db /state/naive-A4.db --mode naive --count 5
docker compose run --rm --no-deps app status --db /state/naive-A4.db
```

- **A4-1.1.3** Vérifier `shipments=3` et deux lignes pour `evt-001`. Interprétation : l'idempotence du producer Kafka ne fusionne pas deux publications applicatives du même fait. Le problème se situe dans le traitement métier.

### 1.2. Relire le même journal avec le traitement idempotent et une base indépendante

- **A4-1.2.1** Lancer le mode sûr avec un autre groupe et une autre base. Le nouveau groupe relit les cinq records conservés ; la base indépendante rend la comparaison équitable.

```sh
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group safe-A4 --db /state/safe-A4.db --count 5
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A4-1.2.2** Vérifier les métriques : `records=5`, `accepted=2`, `duplicate=1`, `retry=4`, `dlq=2`, puis `shipments=2`. Dans `src/domain.ts`, lire `apply` : marqueur et effet sont dans une même transaction SQLite. Un nouvel appel pour le même fait retrouve son marqueur après redémarrage.
- **A4-1.2.3** Créer `reponses-A4.txt` dans `~/kafka` (`nano reponses-A4.txt`), copier la trame et enregistrer la première décision. Ne recopier aucun total déjà visible.

```text
Pourquoi l'effet métier doit partager la transaction de déduplication :
Pourquoi une DLQ doit être confirmée avant le commit de l'entrée :
Décision avant de relancer un événement après incident :
```

## 2. Distinguer les erreurs et inspecter la DLQ

### 2.1. Lire les quarantaines et comparer erreur permanente et erreur transitoire

- **A4-2.1.1** Lire les quatre quarantaines créées par les deux traitements. Résultat attendu : deux erreurs de données et deux pannes persistantes, une de chaque par groupe testé.

```sh
docker compose run --rm app read --topic dev.lab.resilience.dlq.v1 --group audit-dlq-A4 --count 4
```

- **A4-2.1.2** Vérifier les types `PermanentError` et `TransientError`, les coordonnées source et l'événement original. Le contrat invalide n'atteint aucune tentative métier (`attempts=0`) ; la panne persistante atteint trois tentatives. `retry=4` compte seulement les tentatives supplémentaires : deux pour l'événement qui finit par réussir et deux pour celui qui échoue encore.
- **A4-2.1.3** Dans `src/worker.ts`, repérer l'envoi DLQ attendu avant `commit`. Compléter la seconde rubrique du fichier texte. Référence : si la DLQ échoue, le worker doit s'arrêter sans valider l'entrée ; si l'ACK DLQ réussit puis le commit échoue, une relecture peut dupliquer la quarantaine. Ce laboratoire ne revendique pas l'atomicité Kafka entrée/sortie.

## 3. Prouver le rejeu après incident

### 3.1. Rejouer avec un nouveau groupe en conservant l'état métier

- **A4-3.1.1** Relire les cinq records avec une nouvelle identité de groupe, mais la même base sûre. Résultat attendu : aucun nouvel effet, trois doublons détectés, deux DLQ supplémentaires. Les événements invalides ne sont pas marqués comme traités dans la base.

```sh
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group replay-A4 --db /state/safe-A4.db --count 5
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A4-3.1.2** Vérifier `shipments=2`. Interprétation : un nouveau groupe relit les données, il ne remet pas l'état métier à zéro. Changer `event_id` pour contourner la déduplication serait une nouvelle publication métier, avec un risque de double effet.

### 3.2. Simuler un arrêt après SQLite et avant commit puis reprendre le même groupe

- **A4-3.2.1** Publier un seul événement sur le topic de panne isolé, puis déclencher l'incident. La seconde commande doit sortir en erreur : c'est le résultat attendu de la simulation.

```sh
docker compose run --rm app seed crash --topic dev.lab.crash.v1
docker compose run --rm app worker --topic dev.lab.crash.v1 --group crash-A4 --db /state/crash-A4.db --count 1 --crash-after-effect
```

- **A4-3.2.2** Relancer sans injection. Résultat attendu : un doublon, aucun nouvel effet ; l'offset est alors validé. Vérifier une seule ligne en base.

```sh
docker compose run --rm app worker --topic dev.lab.crash.v1 --group crash-A4 --db /state/crash-A4.db --count 1
docker compose run --rm --no-deps app status --db /state/crash-A4.db
```

- **A4-3.2.3** Compléter la dernière rubrique de `reponses-A4.txt`. Référence : identifier le fait, vérifier l'effet déjà produit, corriger la cause et choisir un périmètre de reprise ; ne pas supprimer un record du journal pour « débloquer le job ».

## 4. Contrôler les garanties et leurs limites

### 4.1. Exécuter les tests et comparer les limites au traitement réel

- **A4-4.1.1** Exécuter la suite et vérifier sa réussite. Elle teste notamment l'annulation du marqueur de déduplication si l'effet SQLite échoue.

```sh
docker compose run --rm --no-deps app test
```

- **A4-4.1.2** Lire les limites : retry court bloquant, commit par record, état SQLite partagé dans un seul conteneur actif, déduplication sans purge dans ce laboratoire. Un appel HTTP, un email ou un paiement externe ne partage pas cette transaction. En production, étudier clé d'idempotence du destinataire et pattern outbox/inbox. Si le destinataire n'est pas idempotent, une relecture peut encore provoquer un effet double.

Si le délai du worker expire, vérifier le nom de groupe et le nombre de records. Si les totaux diffèrent, vérifier les publications répétées et l'état des bases. Aucun nettoyage n'est nécessaire pour l'idempotence : conserver l'état en fait partie.
