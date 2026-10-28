# Atelier 6 — Diagnostiquer et décider une reprise applicative

Vous êtes responsable d'application. Les commandes du site retail doivent rejoindre l'application de gestion puis l'entrepôt. Les données clients circulent entre web, magasins, partenaires et plateforme de données. L'objectif est de spécifier un comportement attendu et de décider une reprise maîtrisée. Aucun changement de code n'est nécessaire.

Environnement : Docker local, Kafka 4.1.2, applications Node.js 24.14.0 ; ateliers 4 et 7 réalisés ou corrigés disponibles. Les mêmes principes s'appliquent à Aiven, avec ses outils et droits. Aucun accès au tenant client n'est requis. Livrable : une courte fiche locale `reponses-A6.txt` et des contrôles dans les outils.

## 1. Qualifier un incident au bon niveau

### 1.1. Comparer lag et effet métier sur le flux logistique déjà traité

- **A6-1.1.1** Inspecter le groupe sûr de l'atelier 4 et son état métier. Résultat attendu : le groupe a avancé et deux effets sont enregistrés ; des erreurs ont néanmoins été envoyées en DLQ.

```sh
docker compose run --rm app supervise --topic dev.lab.resilience.v1 --group safe-A4
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A6-1.1.2** Comparer à la référence : un lag nul signifie que le groupe a validé ses positions, pas que toutes les commandes ont été expédiées. Lire aussi la DLQ, les erreurs applicatives, le débit d'effets réussis et le délai entre acceptation et résultat métier. Les compteurs du laboratoire sont propres à une exécution ; ils ne constituent pas un tableau de bord durable.
- **A6-1.1.3** Créer `reponses-A6.txt` dans `~/kafka` (`nano reponses-A6.txt`), copier la trame, enregistrer puis compléter au fil des cas.

```text
Incident : impact, flux et périmètre :
Cause supposée et preuve à obtenir :
Action de reprise, risque de doublon et contrôle final :
Spécification du flux clients : clé, version métier, délai et responsable :
```

### 1.2. Choisir une action adaptée parmi trois incidents de référence

- **A6-1.2.1** Cas A : lag en hausse, consumer actif, appels entrepôt en timeout. Noter l'action. Référence : vérifier la dépendance, borner le retry et protéger l'entrepôt ; davantage de consumers aide seulement si partitions et capacité aval le permettent. Scaler le service Aiven n'élimine pas une panne du destinataire.
- **A6-1.2.2** Cas B : lag nul, DLQ en hausse, devise absente. Référence : corriger producteur/contrat et donnée à la source ; préparer un retraitement ciblé. Ne pas réinitialiser tous les offsets : cela rejouerait aussi des faits déjà réussis.
- **A6-1.2.3** Cas C : arrêt après création d'une expédition, avant commit Kafka. Référence : vérifier l'effet par `event_id` ou clé métier, conserver la déduplication puis reprendre le même groupe. L'atelier 4 prouve le résultat sans créer une seconde expédition.

## 2. Préparer un rejeu contrôlé

### 2.1. Simuler le reset des offsets sur le groupe de formation inactif

- **A6-2.1.1** Vérifier que les commandes terminées de l'atelier 5 n'ont laissé aucun consumer actif, puis demander uniquement le plan de reset. Les commandes visent le broker local et le topic de formation.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group supervision-A5 --members
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group supervision-A5 --topic dev.lab.infra.v1 --reset-offsets --to-earliest --dry-run
```

- **A6-2.1.2** Vérifier les offsets proposés. Interprétation : un reset déplace le pointeur d'un groupe ; il ne supprime aucun message ni effet déjà réalisé. Il faut un groupe inactif et un périmètre convenu. Ne pas ajouter `--execute` pour cet exercice. Pour observer un rejeu sans modifier ce groupe, utiliser un nouveau groupe de lecture sur ce topic local.

### 2.2. Définir le contrôle final d'une reprise et la responsabilité Aiven/application

- **A6-2.2.1** Compléter la rubrique reprise. Référence : identifier topic/partition/offset et `event_id`, garder le diagnostic, corriger la cause, limiter le périmètre, vérifier les effets déjà produits, observer résultat métier et DLQ après reprise. Une rétention expirée n'est pas réparée par un reset ; il faut une procédure de restauration ou une source de reconstruction distincte.
- **A6-2.2.2** Distinguer responsabilités : disponibilité du service selon contrat et opérations de plateforme d'un côté ; sens des événements, contrats, droits demandés, délais applicatifs et reprise métier de l'autre. Vérifier plan Aiven et droits réels avant d'annoncer une fonction disponible. Les fichiers plats d'hier ne justifient pas la suppression d'un record Kafka pour corriger une seule erreur.

## 3. Spécifier le flux clients du SI retail

### 3.1. Définir un fait client sans transférer toute la fiche client

- **A6-3.1.1** Cas : une modification client du web doit rejoindre magasins, partenaires autorisés et plateforme de données. Noter clé, version métier, délai attendu et responsable. Référence : identifiant client stable comme clé, fait explicite tel que `CustomerContactUpdated`, version de l'agrégat ou règle métier pour détecter une mise à jour obsolète, délai de propagation à faire valider et owner du domaine client. Chaque application qui a besoin de tous les faits utilise son propre groupe.
- **A6-3.1.2** Comparer à la référence de minimisation : identifiants et seuls attributs nécessaires ; classification, accès, durée et finalités des données personnelles. Le scénario utilise des données fictives. Une demande de suppression nécessite une conception adaptée ; ni compaction ni rétention courte ne valent suppression individuelle immédiate.
- **A6-3.1.3** Relire la fiche et vérifier quatre critères : effet métier défini, groupe choisi, alerte reliée à un délai, reprise circonscrite. Le parcours est réussi si chaque décision concorde avec les références. Les développeurs peuvent réutiliser ces exigences pour leurs tests d'acceptation.
