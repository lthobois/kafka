# Atelier 4 — Éviter les doublons et isoler les erreurs

Objectif : comparer un traitement naïf à un traitement idempotent, observer la DLQ puis rejouer sans produire un second effet. SQLite simule la prise en charge du PDF ; aucun document binaire n'est généré.

Prérequis : environnement du [README](../README.md) opérationnel avec `app init`. Même VM Linux, Kafka 4.1.2 et application Node.js fournie. Un terminal, aucune modification de code. Publier le jeu une seule fois dans un environnement neuf. Les références **A4-thématique.action.tâche** identifient les tâches.

## 1. Comparer deux traitements du même journal

### 1.1. Publier les cinq records puis exécuter le mode naïf

- **A4-1.1.1** Publier le jeu isolé. Il contient un fait valide, son doublon, une panne transitoire qui finit par réussir, une donnée invalide et une panne persistante.

```sh
cd ~/kafka
docker compose run --rm app seed resilience --topic dev.lab.resilience.v1
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group naive-A4 --db /state/naive-A4.db --mode naive --count 5
docker compose run --rm --no-deps app status --db /state/naive-A4.db
```

- **A4-1.1.2** Vérifier cinq records, `accepted=3`, `duplicate=0`, `retry=4`, `dlq=2` et `tickets_pdf=3`. Pourquoi deux effets pour `evt-001` ? **Réponse :** deux publications applicatives sont deux records ; l'idempotence du producer ne remplace pas la déduplication métier.

### 1.2. Lire le même journal avec le mode sûr et une base distincte

- **A4-1.2.1** Utiliser un autre groupe et une autre base pour comparer équitablement.

```sh
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group safe-A4 --db /state/safe-A4.db --count 5
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A4-1.2.2** Vérifier `accepted=2`, `duplicate=1`, `retry=4`, `dlq=2` et `tickets_pdf=2`. La transaction SQLite enregistre ensemble effet et marqueur `event_id` ; une relecture retrouve le marqueur. Les quatre retries sont les tentatives supplémentaires de deux événements, pas quatre faits métier.

## 2. Examiner les erreurs

### 2.1. Lire les quatre quarantaines des deux modes

- **A4-2.1.1** Lire les résultats des deux expériences avant le rejeu.

```sh
docker compose run --rm app read --topic dev.lab.resilience.dlq.v1 --group audit-dlq-A4 --count 4
```

- **A4-2.1.2** Vérifier deux `PermanentError` et deux `TransientError`, avec les coordonnées source. La donnée invalide ne déclenche aucun essai métier (`attempts=0`) ; la panne persistante atteint trois tentatives. Pourquoi attendre l'ACK DLQ avant le commit ? **Réponse :** conserver le message à sa source si son transfert échoue.

## 3. Rejouer en conservant l'état métier

### 3.1. Relire avec replay-A4 et la base sûre existante

- **A4-3.1.1** Changer seulement le groupe, en conservant la base. Le nouveau groupe relit les records, sans effacer les effets existants.

```sh
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group replay-A4 --db /state/safe-A4.db --count 5
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A4-3.1.2** Vérifier `accepted=0`, `duplicate=3`, `retry=4`, `dlq=2` et toujours `tickets_pdf=2`. La DLQ contient maintenant six records : les deux mêmes échecs ont été observés dans trois expériences. Ce ne sont pas six ventes invalides distinctes.

## 4. Vérifier les limites

### 4.1. Comparer les résultats à la frontière transactionnelle

- **A4-4.1.1** Contrôler la progression 3 effets naïfs, 2 effets sûrs, puis aucun effet supplémentaire au rejeu. Un email, une API assureur ou un paiement ne partage pas la transaction SQLite : chacun exige son mécanisme d'idempotence.
- **A4-4.1.2** Si les comptes diffèrent, vérifier publications, groupes et chemins de base. Conserver la base sûre pour l'[atelier 6](Atelier06_Responsables-application.md). Ne pas supprimer les volumes entre ces deux fiches.
