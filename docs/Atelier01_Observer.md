# Atelier 1 — Analyser un flux existant

Vous reprenez un flux qui mélange commandes et traces techniques. Votre rôle : repérer les risques avant de proposer une organisation. Livrables : observation des groupes et quelques décisions dans `reponses-A1.txt`.

Environnement : projet démarré selon le README, Kafka 4.1.2, client Confluent `@confluentinc/kafka-javascript` 1.11.0 dans Docker, une connexion SSH à la VM et un éditeur (voir le README, section « Travailler sur la VM »). Aucun compte cloud. Utiliser un environnement neuf pour retrouver les totaux de référence. La topologie JSON décrit un système fictif à auditer, et non des applications déjà lancées dans Compose.

Les tâches portent des références uniques en gras ; conserver cette référence pour signaler un blocage. Les décisions se notent dans un fichier texte local, jamais dans cette page Git.

## 1. Diagnostiquer le flux legacy

### 1.1. Lire les données et la topologie legacy pour isoler trois risques métier

- **A1-1.1.1** Ouvrir `data/legacy.json` et `data/legacy-topology.json` dans l'éditeur. Chercher qui écrit, qui lit, quelle clé est utilisée et qui possède le contrat. Pourquoi : une anomalie de design ne se voit pas seulement dans les logs. Résultat attendu : des indices sur le mélange des environnements, les types d'événements et les responsabilités. Vérification : comparer à la référence de l'action suivante.
- **A1-1.1.2** Créer `reponses-A1.txt` dans `~/kafka` (`nano reponses-A1.txt`), copier la trame ci-dessous, noter trois risques et une décision de groupe ; enregistrer. Ne recopier aucun record. Réutiliser ce fichier lors de la synthèse finale de l'atelier.

```text
Trois risques et leurs conséquences :
Décision pour les groupes shipping et analytics, avec justification :
Priorité de correction :
```

Référence : `dev` et `prod` se mélangent ; des traces SQL partagent le topic de commandes ; le montant est tantôt un nombre sans devise, tantôt une chaîne ; aucun `event_id` stable ne distingue un doublon d'un second fait ; la clé et le propriétaire manquent. Shipping et analytics doivent lire tous les faits : ils nécessitent deux groupes distincts. Un même groupe distribuerait les partitions entre eux ; avec une seule partition, un seul consumer serait actif.

### 1.2. Publier les quatre records legacy et comparer deux groupes indépendants

- **A1-1.2.1** Exécuter la publication une seule fois. Résultat attendu : `SEEDED legacy`. Cette publication ajoute des records ; elle ne remplace pas les anciens.

```sh
docker compose run --rm app seed legacy
docker compose run --rm app read --topic legacy-events --group audit-A1 --count 4
docker compose run --rm app read --topic legacy-events --group analytics-A1 --count 4
```

- **A1-1.2.2** Vérifier `READ records=4` pour chaque groupe. Examiner les offsets : ils indiquent une position dans la partition, pas un identifiant métier. Les deux groupes ont lu le même journal indépendamment.
- **A1-1.2.3** Relancer le premier groupe, puis utiliser un nouveau groupe. Résultat attendu : zéro record pour le groupe ayant déjà committé, quatre pour le nouveau. Interprétation : `fromBeginning: true` ne force pas le retour au début d'un groupe ayant un offset valide.

```sh
docker compose run --rm app read --topic legacy-events --group audit-A1 --count 4 --timeout 5
docker compose run --rm app read --topic legacy-events --group replay-A1 --count 4
```

## 2. Comprendre la répartition des partitions

### 2.1. Démarrer deux consumers du même groupe sur un topic à trois partitions

- **A1-2.1.1** Publier le jeu de 30 records. Il impose les partitions pour rendre la démonstration lisible ; un producteur métier utilise normalement la clé et le partitionneur.

```sh
docker compose run --rm app seed groups
```

- **A1-2.1.2** Ouvrir deux connexions SSH supplémentaires vers la VM (ou deux fenêtres `tmux`) et exécuter `cd ~/kafka` dans chacune. Lancer immédiatement la même commande dans les deux. Le délai volontaire ralentit la lecture pour laisser le groupe se rééquilibrer.

```sh
docker compose run --rm app read --topic dev.lab.groups.v1 --group workers-A1 --count 30 --timeout 45 --delay 1
```

- **A1-2.1.3** Dans la première connexion SSH, inspecter le groupe pendant que les lecteurs tournent. Résultat attendu : deux membres actifs, avec une répartition de deux partitions et une partition. Vérifier dans la sortie de la commande. Si un seul membre apparaît, relancer l'observation avec un nouveau nom de groupe dans les deux terminaux en démarrant le second plus tôt.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group workers-A1 --members --verbose
```

- **A1-2.1.4** Attendre la fin des deux lecteurs et comparer leurs sorties. En régime stable, une partition n'est attribuée qu'à un membre du groupe. Le groupe traite 30 positions distinctes au total sur ce premier passage ; un rebalance peut provoquer une relecture, donc comparer les couples partition/offset en cas de doublon. L'ordre est garanti dans chaque partition, pas entre leurs sorties entrelacées. Avec quatre membres pour trois partitions, au moins un membre serait inactif.

## 3. Consolider le diagnostic

### 3.1. Comparer ses décisions au corrigé et identifier la prochaine amélioration

- **A1-3.1.1** Retrouver `reponses-A1.txt`. Compléter seulement la priorité de correction : séparer les environnements et fixer la signification du fait avant d'ajouter des partitions. Vérifier que les deux applications ont des groupes différents et que le besoin d'ordre est formulé par commande.
- **A1-3.1.2** Répondre mentalement : « Lire supprime-t-il les messages ? » Référence : non, la rétention gère leur conservation indépendamment de la lecture. « Un offset est-il global au topic ? » Référence : non, il est propre à une partition. Si ces réponses et les observations précédentes concordent, l'atelier est réussi.

Diagnostic : zéro record indique souvent un groupe déjà passé ; un topic inconnu indique que `app init` manque. Les totaux supposent une seule publication. Ne supprimer aucun volume pour corriger un simple nom de groupe.
