# Laboratoires Kafka pour développeurs

Un flux de ventes en ligne permet d'observer Kafka, de concevoir un événement puis de rendre son traitement rejouable. Tout s'exécute en conteneurs sur une VM Linux à laquelle vous accédez en SSH : un broker Kafka 4.1 et des scripts TypeScript/Node.js qui utilisent le client `@confluentinc/kafka-javascript`.

Aucune installation n'est nécessaire sur votre poste, en dehors d'un client SSH.

## 1. Se connecter à la VM

Le formateur vous communique le nom de votre VM, l'utilisateur et le mot de passe. Depuis un terminal de votre poste (Terminal Windows, PowerShell, macOS ou Linux) :

```sh
ssh dockeradmin@<nom-de-votre-vm>.westeurope.cloudapp.azure.com
```

Répondre `yes` à la première connexion, puis saisir le mot de passe. Toutes les commandes des ateliers s'exécutent ensuite **sur la VM**.

## 2. Récupérer les ateliers

Sur la VM, cloner le dépôt de la formation avec les commandes suivantes. Elles créent le dossier `~/kafka` ; aucune archive n'est à transférer.

```sh
cd ~
git clone https://github.com/lthobois/kafka.git kafka
cd kafka
git branch --show-current
```

Résultat attendu : la branche `main`. Ouvrir les [consignes du dépôt](https://github.com/lthobois/kafka/tree/main/docs) dans le navigateur du poste. Les laisser affichées pendant les manipulations sur la VM.

Toutes les commandes des ateliers se lancent dans ce dossier `~/kafka`.

## Comprendre les commandes du laboratoire

Dans ce dépôt, Docker Compose est le lanceur. Il lit [compose.yaml](compose.yaml), qui décrit deux services : **kafka**, le serveur durable, et **app**, un programme Node.js lancé pour une action puis arrêté. [Dockerfile](Dockerfile) prépare l'image applicative et ses dépendances ; il ne décide pas de l'action métier.

Exemple à décoder, pas une commande supplémentaire à exécuter maintenant :

```text
docker compose run --rm app seed orders --solution --topic dev.lab.design.v1
```

| Partie | Signification ici |
|---|---|
| docker compose | Lire les services de compose.yaml depuis le dossier courant |
| run --rm app | Créer un conteneur ponctuel app et le retirer à sa sortie |
| seed orders | Arguments reçus par node src/cli.ts : publier le jeu de trois billets |
| --solution | Utiliser le contrat et la fonction de clé fournis |
| --topic … | Choisir le topic destinataire ; ce n'est pas une option Docker |

Le service app fixe les deux repères suivants ; lire le [fichier complet](compose.yaml) pour les autres réglages :

```yaml
working_dir: /workspace
entrypoint: [node, src/cli.ts]
```

Le chemin exécuté est donc : **terminal SSH → Compose → conteneur app → src/cli.ts → fonction de publication, lecture ou traitement**. Dans [src/cli.ts](src/cli.ts), parseArgs sépare le nom de commande des options et appelle le bon module. Les fiches donnent directement les extraits à lire : il n'est pas nécessaire de parcourir tout le code.

La publication vers Kafka se trouve dans le helper send de src/bus.ts : il transforme la valeur en JSON, conserve la clé et attend la promesse du client Kafka.

**Code à lire — [src/bus.ts](src/bus.ts#L40)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:40-40 -->
```ts
await p.send({ topic, messages: [{ key, value: JSON.stringify(value), headers: { ...headers, ...traceHeaders() } }] });
```

| Forme rencontrée | Effet à distinguer |
|---|---|
| up -d --wait kafka | Démarrer le serveur en arrière-plan et attendre son état de santé |
| run --rm app … | Lancer une nouvelle exécution du programme du laboratoire |
| run --rm --no-deps app status/test/check | Contrôle local sans démarrer Kafka comme dépendance |
| exec kafka /opt/kafka/bin/… | Lancer un outil officiel dans le conteneur Kafka déjà actif |
| --group … | Choisir la progression Kafka ; ce n'est pas le nom du conteneur |
| --db /state/… | Choisir le fichier SQLite du traitement métier |

**Ce qui reste après --rm.** Le dossier du dépôt est monté dans /workspace ; app-state est monté dans /state et kafka-data conserve les données du broker. Retirer le conteneur d'une commande ne vide donc ni les topics, ni les offsets, ni SQLite. Changer de groupe relit éventuellement Kafka ; cela ne remet pas la base métier à zéro.

Les explications sous « Avant de lancer » font partie de la lecture guidée, sans tâche supplémentaire, fichier de réponses ni code à écrire. Les options sont reprises au moment où elles servent. Références : [Compose run](https://docs.docker.com/reference/cli/docker/compose/run/), [exec](https://docs.docker.com/reference/cli/docker/compose/exec/), [up](https://docs.docker.com/reference/cli/docker/compose/up/) et [down](https://docs.docker.com/reference/cli/docker/compose/down/).

## 3. Préparer et démarrer l'environnement

Si le formateur a déjà préparé la VM, `docker version` répond sans erreur : passer directement au démarrage. Sinon, installer Docker et précharger les images (quelques minutes) :

```sh
sudo bash scripts/preparer-vm.sh
exit
```

La commande `exit` ferme la session : relancer la commande SSH de l'étape 1, puis exécuter les commandes ci-dessous. Cette reconnexion active les nouveaux droits Docker.


<!-- compose-pedagogie -->
**Avant de lancer :**

`docker compose version` affiche la version du plugin Compose installé sur la VM. Aucun service ni code du laboratoire n’est lancé.

```sh
cd ~/kafka
id
docker version
docker compose version
```

Résultat attendu : le groupe `docker` apparaît dans `id`, `docker version` affiche Client et Server, et Compose affiche sa version. Les manipulations suivantes s'exécutent sans `sudo`.

Démarrer Kafka, créer les topics et vérifier l'environnement :


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. `--wait` attend leur état running ou healthy selon le service ; `--wait-timeout 180` borne cette attente en secondes.

Le service kafka démarre le serveur officiel défini dans [compose.yaml](compose.yaml). Le contrôle de santé exécute une liste des topics ; il ne vérifie pas les effets métier. Ni publication ni consumer du laboratoire ne démarrent avec cette seule commande.

**Commande 2 du bloc.** `run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`init` appelle [initTopics](src/bus.ts) puis `createTopics` via l’Admin API. Il crée la liste des topics du laboratoire, avec leurs partitions et leur rétention. Il ne publie aucun record de vente. Le topic design propre à LAB-02 est créé ensuite par sa commande dédiée.
**Code à lire — [src/bus.ts](src/bus.ts#L55)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:55-57 -->
```ts
await createTopics(topics.map(([topic, numPartitions]) => ({ topic, numPartitions,
  replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] })));
console.log('TOPICS OK', topics.map(([name]) => name));
```


**Commande 3 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`--no-deps` évite de démarrer Kafka pour ce contrôle local.

`test` passe par [src/cli.ts](src/cli.ts) puis lance le lanceur `node --test` sur [tests/domain.test.ts](tests/domain.test.ts). Ce contrôle ne publie rien dans Kafka et ne démarre pas de consumer.
**Code à lire — [src/cli.ts](src/cli.ts#L19)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:19-22 -->
```ts
const args = command === 'check' ? [resolve(ROOT, 'node_modules/typescript/bin/tsc'), '--noEmit']
  : ['--test', resolve(ROOT, command === 'test' ? 'tests/domain.test.ts' : 'tests/exercise.test.ts')];
const result = spawnSync(process.execPath, args, { cwd: ROOT, stdio: 'inherit' });
process.exitCode = result.status ?? 1;
```


**Commande 4 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`--no-deps` évite de démarrer Kafka pour ce contrôle local.

`check` passe par [src/cli.ts](src/cli.ts) puis lance TypeScript avec `tsc --noEmit`, pour contrôler les types sans produire de JavaScript. Ce contrôle ne publie rien dans Kafka et ne démarre pas de consumer.

```sh
cd ~/kafka
docker compose up -d --wait --wait-timeout 180 kafka
docker compose run --rm app init
docker compose run --rm --no-deps app test
docker compose run --rm --no-deps app check
```

Résultat attendu : `Container kafka-formation-kafka-1 Healthy`, puis `TOPICS OK`, `TESTS OK` et `CHECK OK`. Ouvrir ensuite l'atelier 1 dans le tableau du parcours ci-dessous. Toutes ses commandes sont fournies dans la fiche ; revenir ensuite à ce tableau pour suivre l'ordre indiqué.

## 4. Travailler sur la VM

**Ouvrir plusieurs terminaux.** Le parcours de la journée utilise un seul terminal. Les fiches complémentaires peuvent demander plusieurs terminaux. Ouvrir autant de connexions SSH depuis votre poste, et dans chacune exécuter `cd ~/kafka`. Alternative dans une seule connexion : `tmux`, puis `Ctrl+b c` pour une nouvelle fenêtre et `Ctrl+b n` pour passer à la suivante.

**Lire le code fourni.** Les quatre ateliers ne demandent aucune modification de fichier.

- Dans le terminal : `less src/bus.ts` ou `less contracts/ticket-sold.v1.json`, quitter avec `q`.
- Plus confortable : Visual Studio Code avec l'extension **Remote - SSH**. Commande `Remote-SSH: Connect to Host…`, saisir `dockeradmin@<nom-de-votre-vm>.westeurope.cloudapp.azure.com`, puis ouvrir le dossier `/home/dockeradmin/kafka`.

**Suivre les références.** Chaque tâche fournit la manipulation, le résultat attendu et son explication. Les questions sont suivies de leur réponse.

**Lire les consignes.** Les fiches d'atelier s'affichent directement sur la page GitHub du dépôt, dans le dossier `docs`. Les garder ouvertes dans votre navigateur à côté du terminal.

## Les six ateliers

Les six fiches sont accessibles ci-dessous. Un seul terminal suffit pour le parcours de la journée ; les réponses et les résultats attendus sont fournis.

| Atelier | Travail | Consignes | Parcours |
|---|---|---|---|
| Atelier 1 | Lire le même journal avec deux groupes et reprendre une lecture | [Lire le journal](docs/Atelier01_Observer.md) | Journée |
| Atelier 2 | Créer un topic, publier les billets fournis et observer le lag | [Publier et observer](docs/Atelier02_Concevoir.md) | Journée |
| Atelier 3 | Comprendre les responsabilités et contrôler un catalogue | [Gouverner les flux](docs/Atelier03_Gouverner.md) | Complément |
| Atelier 4 | Comparer les doublons, lire la DLQ et rejouer sans nouvel effet | [Fiabiliser](docs/Atelier04_Fiabiliser.md) | Journée |
| Atelier 5 | Configurer les partitions, la rétention et la supervision d'un topic | [Configurer les topics](docs/Atelier05_Topics.md) | Complément |
| Atelier 6 | Diagnostiquer et afficher un plan de reprise | [Préparer la reprise](docs/Atelier06_Responsables-application.md) | Journée |

Terminer par la [synthèse guidée avec ses réponses](docs/Evaluation.md).

Les numéros identifient les fiches ; l'ordre de la journée est celui du tableau.

Les références en gras, par exemple `A2-1.1.1`, identifient une tâche unique. En cas de blocage, transmettre cette référence au formateur. Chaque tâche indique le résultat attendu : vous vérifiez vous-même dans l'outil ou en comparant à la référence fournie.

## Organisation du code

- `src/bus.ts` : connexion à Kafka, producer et consumer ; les accusés de réception des publications sont contrôlés.
- `exercises/routing.ts` et `exercises/ticket-sold.v1.json` : clé et contrat de référence.
- `src/worker.ts` et `src/domain.ts` : traitement génération PDF, en mode naïf ou sûr, avec stockage SQLite.
- `src/queues.ts` : traitement avec topic de retry et DLQ.
- `contracts` et `solutions` : contrat, clé et catalogue corrigés.
- `tests` : tests de référence et tests d'acceptation des exercices.
- `data` : données synthétiques et topologie du flux existant à analyser.

Le code sert à rendre les mécanismes de Kafka visibles ; vous n'avez pas à l'écrire. [Les trois projets](projets/README.md) décrivent leurs responsabilités et donnent les explications de référence. [Service managé et observabilité](docs/Aiven-et-observabilite.md) montre la transposition vers un Kafka managé et l'activation des traces.

## Reprendre ou arrêter

Les volumes conservent Kafka et les bases SQLite, même après `docker compose down` ou un redémarrage de la VM. Un consumer group reprend à ses offsets ; pour relire, choisir un nouveau `--group`. Un nouveau groupe ne vide pas SQLite : c'est précisément ce qui permet de tester l'idempotence.

Après une reconnexion ou un redémarrage de la VM, relancer Kafka :


<!-- compose-pedagogie -->
**Avant de lancer :**

`up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. `--wait` attend leur état running ou healthy selon le service ; `--wait-timeout 180` borne cette attente en secondes.

Le service kafka démarre le serveur officiel défini dans [compose.yaml](compose.yaml). Le contrôle de santé exécute une liste des topics ; il ne vérifie pas les effets métier. Ni publication ni consumer du laboratoire ne démarrent avec cette seule commande.

```sh
cd ~/kafka
docker compose up -d --wait --wait-timeout 180 kafka
```

Ne republier un jeu de données qu'en cas de besoin : chaque publication ajoute des records. Les résultats attendus supposent une seule publication de chaque jeu.

Nettoyage **destructif**, uniquement pour recommencer tous les ateliers depuis le début : la commande suivante supprime les topics, les offsets et les bases SQLite (l'option `--profile tools` inclut le volume des bases). Elle ne modifie pas vos fichiers.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `--profile tools` rend disponibles les services de ce profil.

`down` arrête et retire les conteneurs et le réseau du projet. **Avec -v, les volumes nommés déclarés sont supprimés : perte des records/offsets Kafka et des bases SQLite du laboratoire.** Les images et les fichiers du dépôt restent présents.

**Commande 2 du bloc.** `up` crée ou démarre les services demandés. `-d` les laisse en arrière-plan. `--wait` attend leur état running ou healthy selon le service ; `--wait-timeout 180` borne cette attente en secondes.

Le service kafka démarre le serveur officiel défini dans [compose.yaml](compose.yaml). Le contrôle de santé exécute une liste des topics ; il ne vérifie pas les effets métier. Ni publication ni consumer du laboratoire ne démarrent avec cette seule commande.

**Commande 3 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`init` appelle [initTopics](src/bus.ts) puis `createTopics` via l’Admin API. Il crée la liste des topics du laboratoire, avec leurs partitions et leur rétention. Il ne publie aucun record de vente. Le topic design propre à LAB-02 est créé ensuite par sa commande dédiée.

```sh
docker compose --profile tools down -v
docker compose up -d --wait --wait-timeout 180 kafka
docker compose run --rm app init
```

## Diagnostic rapide

| Symptôme | Vérification et correction |
|---|---|
| `permission denied` sur `/var/run/docker.sock` | Se déconnecter puis se reconnecter en SSH après `preparer-vm.sh` |
| `docker: command not found` | Lancer `sudo bash scripts/preparer-vm.sh` |
| `no configuration file provided` | Revenir dans le dossier du dépôt : `cd ~/kafka` |
| Kafka ne devient pas sain | `docker compose logs kafka` ; vérifier l'espace disque avec `df -h` |
| `Unknown topic` | Exécuter `docker compose run --rm app init` |
| `READ records=0` | Le groupe a déjà lu les records ; utiliser un nouveau `--group` ou publier le jeu manquant |
| Le worker atteint son délai | Vérifier topic, nombre de records, groupe et publication du jeu |
| Des totaux dépassent le corrigé | Le jeu a été publié plusieurs fois ; comparer les identifiants ou recommencer après nettoyage |
| `exercise-tests` échoue | Le parcours utilise directement --solution ; les exercices de code restent en complément |
| La connexion SSH se coupe pendant une commande | Se reconnecter ; les données sont conservées. Utiliser `tmux` pour les commandes longues |

Kafka n'expose aucun port sur la VM : les scripts utilisent `kafka:9092` sur le réseau interne de Docker Compose. Le laboratoire fonctionne en clair sur un broker unique ; ce n'est pas un modèle de déploiement de production. Les contrats sont validés dans l'application, sans Schema Registry.

## Contrôles formateur, hors parcours participant

Pour vérifier l'ensemble de l'environnement en une fois (formateur, ou après une préparation de VM) :


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`integration` exécute [integration](src/integration.ts) : publier des jeux sur des topics uniques, exécuter les workers et comparer les métriques à des assertions. Il crée des données de test isolées ; il ne rejoue pas les groupes des ateliers.
**Code à lire — [src/integration.ts](src/integration.ts#L22)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/integration.ts:22-22 -->
```ts
assert.deepEqual(metrics, { records: 5, accepted: 2, duplicate: 1, retry: 4, dlq: 2 });
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`transactions` appelle [transactions](src/experiments.ts) : créer un topic de test unique, annuler une publication, en valider une autre puis comparer read_uncommitted et read_committed. Les assertions contrôlent les résultats ; aucun offset des ateliers n’est réinitialisé.
**Code à lire — [src/experiments.ts](src/experiments.ts#L10)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/experiments.ts:10-13 -->
```ts
let tx = await p.transaction();
await send(tx, topic, 'TKT-X', { status: 'aborted' }); await tx.abort();
tx = await p.transaction();
await send(tx, topic, 'TKT-Y', { status: 'committed' }); await tx.commit();
```


```sh
docker compose run --rm app integration
docker compose run --rm app transactions
```

Résultat attendu : `INTEGRATION OK` puis `TRANSACTIONS OK`. Ces recettes utilisent leurs propres topics et ne modifient pas les données des ateliers. Le workflow `.github/workflows/verify.yml` exécute la même recette sur GitHub Actions à chaque modification du dépôt.

## Billetterie et ressources complémentaires

Le topic logique est `ticket.sold`, le topic du socle `dev.ticketing.ticket.sold.v1`. `ticketId` identifie le billet, `eventId` le concert, `event_id` le fait et `orderId` la commande. `insuranceSelected` exprime la souscription. La commande historique `seed orders` publie désormais des billets. Le worker inscrit un effet simulé dans `tickets_pdf` ; il ne génère pas de binaire PDF et n'envoie aucun email.

Les corrigés restent directement dans les fiches. Les ressources suivantes sont hors parcours de la journée :

| Complément | Fiche |
|---|---|
| Retry par topic séparé | [Atelier 7](docs/Atelier07_Retry-queues.md) |
| Charge et panne d'un broker | [Atelier 8](docs/Atelier08_Capacite-et-resilience.md) |
| Streams et Connect | [Atelier 9](docs/Atelier09_Streams-et-Connect.md) |
| Transactions, rétention courte et compaction | [Extensions](docs/Extensions.md) |

Ces compléments ne sont ni des prérequis ni des étapes à terminer pendant la journée. Les VM sont préparées avant les manipulations ; le guide formateur décrit les contrôles techniques.
