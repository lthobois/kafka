# Atelier 3 — Mettre les flux en cohérence, facultatif

Vous préparez l'ouverture d'un flux aux applications du SI retail. Même avec Kafka managé par Aiven, le producteur et les responsables d'application doivent s'accorder sur le sens, les accès et la reprise. Livrable : `exercises/catalog.json` conforme au contrôle local.

Environnement : dépôt cloné sur la VM et éditeur, image Node.js 24.14.0, client Confluent `@confluentinc/kafka-javascript` 1.11.0. Les contrôles de catalogue ne nécessitent pas de broker. Les données sont synthétiques. Le formateur a préconstruit l'image. Aucun compte Aiven n'est nécessaire.

Les références en gras identifient les tâches. Si vous ne réalisez pas cet atelier, lisez `solutions/catalog.json` et appliquez les mêmes questions au cas client de l'atelier responsables d'application. Aucun fichier de réponses supplémentaire n'est nécessaire.

## 1. Définir les responsabilités

### 1.1. Comparer le catalogue incomplet au standard de flux applicatif

- **A3-1.1.1** Ouvrir `exercises/catalog.json` et lancer le contrôle. Résultat attendu : une liste d'écarts et un code de sortie non nul. Interprétation : la création technique d'un topic ne suffit pas à rendre le flux exploitable.

```sh
docker compose run --rm --no-deps app catalog
```

- **A3-1.1.2** Lire `solutions/catalog.json`. Vérifier que chaque topic a un owner, un but, une clé, un schéma, des consommateurs, une classification, un contact incident, des partitions et une rétention. Référence : l'équipe commandes possède le fait ; la logistique possède le traitement et la DLQ ; la plateforme applique les règles de service et d'accès. Un responsable d'application définit le délai acceptable et les conséquences d'une reprise.

## 2. Aligner le catalogue

### 2.1. Compléter les deux entrées commande et DLQ puis exécuter le contrôle

- **A3-2.1.1** Modifier `exercises/catalog.json` en prenant le catalogue solution comme référence : deux topics, noms canoniques, clé `order_id`, rétention de sept jours, trois partitions et champs d'exploitation complets. Le nombre de partitions est un choix de laboratoire ; il ne vaut pas dimensionnement de production.
- **A3-2.1.2** Relancer `app catalog`. Résultat attendu et vérification : `CATALOG OK`. En cas de blocage, copier la solution avec `cp solutions/catalog.json exercises/catalog.json` puis relancer. Interprétation : le contrôle valide la présence et quelques règles de cohérence, pas la pertinence métier ni les droits effectifs Aiven.

## 3. Préparer la demande d'ouverture

### 3.1. Confronter le catalogue au circuit de validation du flux

- **A3-3.1.1** Comparer votre catalogue à la grille : sens du fait validé par le domaine ; contrat et évolution portés par le producteur ; test de compatibilité côté consommateurs ; classification et accès approuvés selon les règles internes ; paramètres et quotas appliqués par la plateforme ; alertes et reprise connues du responsable d'application. Résultat attendu : identifier le décideur de chaque point, sans attribuer à Aiven la définition de votre contrat métier.
- **A3-3.1.2** Vérifier mentalement le cas « un nouveau consommateur demande tous les événements ». Référence : nouveau groupe, droits de lecture adaptés et accord sur l'usage des données ; ce besoin ne justifie pas automatiquement un nouveau topic. L'atelier est réussi si le catalogue passe et si cette distinction est claire.
