# Atelier 6 — Diagnostiquer et préparer une reprise

Objectif : distinguer transport et résultat métier, puis observer un plan de reset sans l'exécuter. Les décisions et les réponses sont fournies dans cette fiche.

Prérequis : ateliers [2](Atelier02_Concevoir.md) et [4](Atelier04_Fiabiliser.md) terminés, même environnement conservé. VM Linux, Docker et Kafka 4.1.2 ; un terminal. Aucun compte cloud, aucune modification de code et aucun écrit demandé. Les références **A6-thématique.action.tâche** identifient les tâches.

## 1. Comparer transport et résultat métier

### 1.1. Superviser safe-A4 et vérifier sa base

- **A6-1.1.1** Afficher les positions du groupe et les effets réellement enregistrés.

```sh
cd ~/kafka
docker compose run --rm app supervise --topic dev.lab.resilience.v1 --group safe-A4
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A6-1.1.2** Vérifier un lag nul sur les partitions traitées et `tickets_pdf=2`. Le flux est-il entièrement réussi ? **Réponse : non.** Les erreurs ont été transférées en DLQ. Le contrôle de santé doit associer positions, quarantaines et délai du résultat métier.

### 1.2. Lire les actions de référence pour trois incidents

- **A6-1.2.1** Lag en hausse et service PDF en timeout : **action fournie**, vérifier la dépendance et borner les retries. Ajouter des consumers n'aide que si partitions et capacité aval le permettent.
- **A6-1.2.2** Lag nul et devise absente dans les rejets : **action fournie**, corriger contrat et donnée à la source, puis préparer une reprise ciblée. Ne pas rejouer toutes les ventes déjà réussies.
- **A6-1.2.3** Arrêt après l'effet mais avant commit : **action fournie**, conserver la déduplication et reprendre le groupe. Une relecture retrouve le marqueur ; le rejeu de l'atelier 4 montre pourquoi elle ne doit pas recréer l'effet.

## 2. Observer un plan de reprise

### 2.1. Vérifier design-A2 inactif puis demander un reset en dry-run

- **A6-2.1.1** Vérifier qu'aucune lecture de l'atelier 2 ne tourne encore. Exécuter les deux commandes ; `--dry-run` affiche un plan sans déplacer les offsets.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group design-A2 --members
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group design-A2 --topic dev.lab.design.v1 --reset-offsets --to-earliest --dry-run
```

- **A6-2.1.2** Vérifier l'absence de membres actifs et les offsets proposés. Pourquoi ne pas exécuter le reset ? **Réponse :** l'objectif est de lire le plan ; un reset réel ferait relire des événements et pourrait répéter des effets non idempotents. Ne pas ajouter `--execute`.

## 3. Contrôler le raisonnement de reprise

### 3.1. Comparer la procédure au résultat observé

- **A6-3.1.1** Référence : cause corrigée, périmètre limité, données encore conservées, effets existants identifiés, déduplication préservée, puis contrôle du résultat métier. Un reset ne restaure pas les données sorties de la rétention et n'annule aucun effet.
- **A6-3.1.2** Validation : deux effets uniques sont conservés ; le plan de reset a été affiché sans exécution ; chaque incident dispose d'une action expliquée. Terminer avec les [questions et réponses de synthèse](Evaluation.md).
