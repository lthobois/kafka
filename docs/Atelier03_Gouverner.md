# Atelier 3 — Mettre les flux en cohérence, facultatif

Ressource complémentaire, hors parcours de la journée. Aucun atelier du socle ne nécessite de la terminer.

Vous préparez l'ouverture d'un flux aux applications du SI billetterie. Même avec Kafka managé par Aiven, le producteur et les responsables d'application doivent s'accorder sur le sens, les accès et la reprise. Livrable : `exercises/catalog.json` conforme au contrôle local.

Environnement : dépôt cloné sur la VM et éditeur, image Node.js 24.14.0, client Confluent `@confluentinc/kafka-javascript` 1.11.0. Les contrôles de catalogue ne nécessitent pas de broker. Les données sont synthétiques. Le formateur a préconstruit l'image. Aucun compte Aiven n'est nécessaire.

Les références en gras identifient les tâches. Si vous ne réalisez pas cet atelier, lisez `solutions/catalog.json` et comparez-le aux explications fournies dans l’atelier responsables d’application.

## 1. Définir les responsabilités

### 1.1. Comparer le catalogue incomplet au standard de flux applicatif

- **A3-1.1.1** Ouvrir `exercises/catalog.json` et lancer le contrôle. Résultat attendu : une liste d'écarts et un code de sortie non nul. Interprétation : la création technique d'un topic ne suffit pas à rendre le flux exploitable.


<!-- compose-pedagogie -->
**Avant de lancer :**

`run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`--no-deps` évite de démarrer Kafka pour ce contrôle local.

`catalog` appelle [checkCatalog](../src/domain.ts) sur [exercises/catalog.json](../exercises/catalog.json) par défaut. Il vérifie le nom, la clé et les champs d’exploitation du fichier. Il ne crée ni ne modifie aucun topic ; le code de sortie est non nul si des écarts subsistent.
**Code à lire — [src/cli.ts](../src/cli.ts#L27)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:27-29 -->
```ts
const errors = checkCatalog(resolve(ROOT, argument ?? 'exercises/catalog.json'));
console.log(errors.length ? errors.join('\n') : 'CATALOG OK');
process.exitCode = errors.length ? 1 : 0;
```


```sh
docker compose run --rm --no-deps app catalog
```

- **A3-1.1.2** Lire `solutions/catalog.json`. Vérifier que chaque topic a un owner, un but, une clé, un schéma, des consommateurs, une classification, un contact incident, des partitions et une rétention. Référence : l'équipe ventes possède le fait ; le service PDF possède le traitement et la DLQ ; la plateforme applique les règles de service et d'accès. Un responsable d'application définit le délai acceptable et les conséquences d'une reprise.

## 2. Aligner le catalogue

### 2.1. Compléter les deux entrées vente et DLQ puis exécuter le contrôle

- **A3-2.1.1** Copier le catalogue fourni avec `cp solutions/catalog.json exercises/catalog.json`. Référence : deux topics, noms canoniques, clé `ticketId`, sept jours de rétention, trois partitions et champs d’exploitation complets. Ce nombre de partitions est un choix de laboratoire.
- **A3-2.1.2** Relancer `app catalog`. Résultat attendu et vérification : `CATALOG OK`. En cas de blocage, copier la solution avec `cp solutions/catalog.json exercises/catalog.json` puis relancer. Interprétation : le contrôle valide la présence et quelques règles de cohérence, pas la pertinence métier ni les droits effectifs Aiven.

## 3. Préparer la demande d'ouverture

### 3.1. Confronter le catalogue au circuit de validation du flux

- **A3-3.1.1** Comparer votre catalogue à la grille : sens du fait validé par le domaine ; contrat et évolution portés par le producteur ; test de compatibilité côté consommateurs ; classification et accès approuvés selon les règles internes ; paramètres et quotas appliqués par la plateforme ; alertes et reprise connues du responsable d'application. Résultat attendu : identifier le décideur de chaque point, sans attribuer à Aiven la définition de votre contrat métier.
- **A3-3.1.2** Un nouveau consommateur demande tous les événements : faut-il un nouveau topic ? **Réponse :** commencer par un groupe distinct, les droits adaptés et un usage des données convenu. Un nouveau topic n’est pas automatiquement nécessaire.
