# Atelier 2 — Concevoir et publier un événement métier

Une commande acceptée doit alimenter la logistique et l'analyse sans dépendre des tables internes de checkout. Vous concevez le fait `OrderCreated`, puis publiez trois événements. Livrables : contrat JSON complété, clé corrigée, records vérifiés et décisions minimales dans `reponses-A2.txt`.

Préparation : socle du README opérationnel sur la VM, connexion SSH, éditeur (`nano` ou VS Code Remote - SSH) et données synthétiques. Versions : Kafka 4.1.2, Node.js 24.14.0 dans l'image, client Confluent `@confluentinc/kafka-javascript` 1.11.0. Aucun service cloud ni Schema Registry ; la validation JSON est exécutée par l'application. Cet atelier peut commencer sans avoir terminé l'atelier 1.

Les références en gras indiquent les tâches. Les commandes s'exécutent sur la VM, dans le dossier `~/kafka` ; les réponses restent dans un fichier texte de ce dossier.

## 1. Définir le fait et son contrat

### 1.1. Traduire la commande acceptée en événement immuable et choisir le topic

- **A2-1.1.1** Lire le cas : « La commande ORD-001 a été acceptée pour 25 EUR, quantité 1. La logistique doit préparer son expédition. » Distinguer le fait de l'ordre « prépare cette commande ». Résultat attendu : `OrderCreated` décrit ce qui s'est produit ; la réaction logistique appartient au consumer.
- **A2-1.1.2** Créer `reponses-A2.txt` dans `~/kafka` (`nano reponses-A2.txt`), copier la trame, décider et enregistrer. Le nom de référence est `dev.shop.order-created.v1`, la clé est `order_id`. Le domaine est `shop`, l'environnement est `dev`, la version majeure représente le contrat.

```text
Fait métier et moment où il devient vrai :
Topic et clé, justification du besoin d'ordre :
Décision de migration si currency devient amount_currency :
```

Référence : regrouper les faits d'une commande grâce à `order_id`. La clé aide à conserver leur ordre dans une partition tant que le partitionnement reste stable. Ajouter des partitions peut changer leur affectation ; ne pas promettre une continuité d'ordre sans stratégie de migration. En production, des clusters et droits séparés assurent l'isolation ; un préfixe seul ne la garantit pas.

### 1.2. Compléter le contrat JSON de l'exercice et vérifier les valeurs métier

- **A2-1.2.1** Lancer les tests de l'exercice avant modification. Résultat attendu : échecs sur la clé et les contraintes métier ; ce sont les critères à satisfaire.

```sh
docker compose run --rm --no-deps app exercise-tests
```

- **A2-1.2.2** Modifier `exercises/order-created.v1.json` : dans `data.required`, exiger `order_id`, `quantity`, `currency`, `total_cents`. Dans `data.properties`, définir `quantity` entier supérieur ou égal à 1, `currency` parmi `EUR` et `USD`, `total_cents` entier supérieur ou égal à 0. Ajouter `channel` comme chaîne facultative. Conserver `additionalProperties: true` pour que ce lecteur tolère les champs inconnus. Pourquoi : éviter des montants ambigus et permettre un ajout facultatif.
- **A2-1.2.3** Modifier `exercises/routing.ts` pour renvoyer `event.data.order_id`. Relancer les tests. Résultat attendu : `OK`. Vérification complémentaire : comparer votre schéma à `contracts/order-created.v1.json` et votre fonction à `solutions/routing.ts`. Ces deux fichiers servent aussi de point de reprise : `cp contracts/order-created.v1.json exercises/order-created.v1.json` et `cp solutions/routing.ts exercises/routing.ts`.

## 2. Publier et observer le contrat en action

### 2.1. Publier trois commandes avec la clé order_id et lire leur enveloppe

- **A2-2.1.1** Exécuter les commandes. La publication valide votre contrat avant l'envoi. L'ACK est contrôlé ; une erreur de livraison arrête la commande.

```sh
docker compose run --rm app seed orders
docker compose run --rm app read --topic dev.shop.order-created.v1 --group design-A2 --count 3
```

- **A2-2.1.2** Vérifier trois records et les clés `ORD-001`, `ORD-002`, `ORD-003`. Identifier `event_id`, `occurred_at`, `correlation_id` et les données métier. Interprétation : `event_id` identifie le fait, `order_id` identifie l'agrégat ; les deux n'ont pas le même rôle. La distribution exacte entre partitions n'est pas un critère de réussite.

### 2.2. Tester un ajout facultatif et simuler une rupture de contrat

- **A2-2.2.1** Dans l'éditeur, ouvrir `tests/exercise.test.ts` et lire `ancien événement et ajout facultatif` : le même lecteur accepte un ancien événement et un événement avec `channel`. Vérifier en relançant `exercise-tests`. Résultat attendu : les deux passent ; l'ajout facultatif conserve ici la lecture des anciens messages.
- **A2-2.2.2** Retirer temporairement `currency` de la liste `data.required` du contrat d'exercice, puis relancer les tests. Résultat attendu : échec sur le champ manquant. Interprétation : affaiblir le contrat peut laisser publier un message qui casse un consommateur existant. Restaurer le champ et retrouver `OK`.
- **A2-2.2.3** Reprendre `reponses-A2.txt` et compléter la migration de `currency` vers `amount_currency`. Référence : ne pas renommer silencieusement ; ajouter un champ, migrer les producteurs et lecteurs avec une période de coexistence, puis retirer seulement dans un contrat majeur distinct si nécessaire. La compatibilité dépend de tous les lecteurs, pas seulement de ce test local.

## 3. Contrôler le résultat

### 3.1. Vérifier le contrat, la clé et les décisions de version

- **A2-3.1.1** Exécuter les tests de référence et ceux de l'exercice. Résultat attendu : `OK` dans les deux suites. Vérification : vous devez aussi pouvoir pointer une contrainte de montant, la clé stable et un exemple de rupture dans vos fichiers.

```sh
docker compose run --rm --no-deps app test
docker compose run --rm --no-deps app exercise-tests
```

- **A2-3.1.2** Comparer les trois décisions du fichier texte aux réponses de référence. Une correction d'un fait déjà publié se représente par un nouveau fait explicite, par exemple `OrderAmountCorrected`, avec une nouvelle identité ; elle ne consiste pas à modifier un ancien record. L'atelier est réussi si contrat, publication et raisonnement de version concordent.

En cas d'erreur, le message du validateur donne le champ fautif. Si aucun record n'arrive, utiliser un groupe neuf. Les tests couvrent les exemples du laboratoire ; ils ne remplacent pas une politique de compatibilité de schémas en production.
