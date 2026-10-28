# Laboratoires Kafka pour développeurs

Un flux de commandes en ligne permet d'observer Kafka, de concevoir un événement puis de rendre son traitement rejouable. Tout s'exécute en conteneurs sur une VM Linux à laquelle vous accédez en SSH : un broker Kafka 4.1 et des scripts TypeScript/Node.js qui utilisent le client `@confluentinc/kafka-javascript`.

Aucune installation n'est nécessaire sur votre poste, en dehors d'un client SSH.

## 1. Se connecter à la VM

Le formateur vous communique le nom de votre VM, l'utilisateur et le mot de passe. Depuis un terminal de votre poste (Terminal Windows, PowerShell, macOS ou Linux) :

```sh
ssh dockeradmin@<nom-de-votre-vm>.westeurope.cloudapp.azure.com
```

Répondre `yes` à la première connexion, puis saisir le mot de passe. Toutes les commandes des ateliers s'exécutent ensuite **sur la VM**.

## 2. Récupérer les ateliers

```sh
git clone https://github.com/lthobois/kafka.git
cd kafka
```

Toutes les commandes des ateliers se lancent dans ce dossier `~/kafka`.

## 3. Préparer et démarrer l'environnement

Si le formateur a déjà préparé la VM, `docker version` répond sans erreur : passer directement au démarrage. Sinon, installer Docker et précharger les images (quelques minutes) :

```sh
sudo bash scripts/preparer-vm.sh
exit
```

La commande `exit` ferme la session : se reconnecter en SSH pour que les droits Docker s'appliquent, puis revenir dans `~/kafka`.

Démarrer Kafka, créer les topics et vérifier l'environnement :

```sh
cd ~/kafka
docker compose up -d --wait --wait-timeout 180 kafka
docker compose run --rm app init
docker compose run --rm --no-deps app test
```

Résultat attendu : `Container kafka-formation-kafka-1 Healthy`, puis `TOPICS OK` avec la liste des topics, puis `TESTS OK`.

## 4. Travailler sur la VM

**Ouvrir plusieurs terminaux.** Certains ateliers demandent deux ou trois terminaux en même temps. Ouvrir autant de connexions SSH depuis votre poste, et dans chacune exécuter `cd ~/kafka`. Alternative dans une seule connexion : `tmux`, puis `Ctrl+b c` pour une nouvelle fenêtre et `Ctrl+b n` pour passer à la suivante.

**Lire et modifier un fichier.** Les consignes demandent d'ouvrir ou de modifier quelques fichiers.

- Dans le terminal : `nano exercises/routing.ts` ; enregistrer avec `Ctrl+o` puis `Entrée`, quitter avec `Ctrl+x`. Pour seulement lire : `less data/legacy.json`, quitter avec `q`.
- Plus confortable : Visual Studio Code avec l'extension **Remote - SSH**. Commande `Remote-SSH: Connect to Host…`, saisir `dockeradmin@<nom-de-votre-vm>.westeurope.cloudapp.azure.com`, puis ouvrir le dossier `/home/dockeradmin/kafka`.

**Noter ses décisions.** Quand un atelier demande un fichier de réponses, le créer dans `~/kafka`, par exemple `nano reponses-A1.txt`. Ces fichiers ne sont pas suivis par Git. Pour les conserver après la formation, les afficher avec `cat reponses-A1.txt` et copier le texte, ou les récupérer depuis votre poste avec `scp dockeradmin@<nom-de-votre-vm>.westeurope.cloudapp.azure.com:kafka/reponses-*.txt .`

**Lire les consignes.** Les fiches d'atelier s'affichent directement sur la page GitHub du dépôt, dans le dossier `docs`. Les garder ouvertes dans votre navigateur à côté du terminal.

## Parcours

| Étape | Travail | Consignes |
|---|---|---|
| Atelier 1 | Diagnostiquer un flux existant et observer les groupes | [Atelier01_Observer.md](docs/Atelier01_Observer.md) |
| Atelier 5 | Configurer un topic et lire les offsets | [Atelier05_Topics.md](docs/Atelier05_Topics.md) |
| Atelier 2 | Compléter un contrat et publier avec une clé stable | [Atelier02_Concevoir.md](docs/Atelier02_Concevoir.md) |
| Atelier 3, facultatif | Mettre le catalogue en cohérence | [Atelier03_Gouverner.md](docs/Atelier03_Gouverner.md) |
| Atelier 4 | Tester doublons, retries, DLQ et incident | [Atelier04_Fiabiliser.md](docs/Atelier04_Fiabiliser.md) |
| Atelier 7 | Traiter les retry queues et dead-letter queues | [Atelier07_Retry-queues.md](docs/Atelier07_Retry-queues.md) |
| Atelier 6 | Diagnostiquer et préparer une reprise | [Atelier06_Responsables-application.md](docs/Atelier06_Responsables-application.md) |
| Fin de journée | Évaluer ses acquis avec un quiz corrigé | [Evaluation.md](docs/Evaluation.md) |
| Après le parcours | Transactions, rétention et compaction | [Extensions.md](docs/Extensions.md) |

Les numéros identifient les fiches ; l'ordre de la journée est celui du tableau.

Les références en gras, par exemple `A2-1.1.1`, identifient une tâche unique. En cas de blocage, transmettre cette référence au formateur. Chaque tâche indique le résultat attendu : vous vérifiez vous-même dans l'outil ou en comparant à la référence fournie.

## Organisation du code

- `src/bus.ts` : connexion à Kafka, producer et consumer ; les accusés de réception des publications sont contrôlés.
- `exercises/routing.ts` et `exercises/order-created.v1.json` : clé et contrat à compléter dans l'atelier 2.
- `src/worker.ts` et `src/domain.ts` : traitement logistique, en mode naïf ou sûr, avec stockage SQLite.
- `src/queues.ts` : traitement avec topic de retry et DLQ.
- `contracts` et `solutions` : contrat, clé et catalogue corrigés.
- `tests` : tests de référence et tests d'acceptation des exercices.
- `data` : données synthétiques et topologie du flux existant à analyser.

Le code sert à rendre les mécanismes de Kafka visibles ; vous n'avez pas à l'écrire. [Les trois projets](projets/README.md) décrivent leurs responsabilités et proposent des défis facultatifs. [Service managé et observabilité](docs/Aiven-et-observabilite.md) montre la transposition vers un Kafka managé et l'activation des traces.

## Reprendre ou arrêter

Les volumes conservent Kafka et les bases SQLite, même après `docker compose down` ou un redémarrage de la VM. Un consumer group reprend à ses offsets ; pour relire, choisir un nouveau `--group`. Un nouveau groupe ne vide pas SQLite : c'est précisément ce qui permet de tester l'idempotence.

Après une reconnexion ou un redémarrage de la VM, relancer Kafka :

```sh
cd ~/kafka
docker compose up -d --wait --wait-timeout 180 kafka
```

Ne republier un jeu de données qu'en cas de besoin : chaque publication ajoute des records. Les résultats attendus supposent une seule publication de chaque jeu.

Nettoyage **destructif**, uniquement pour recommencer tous les ateliers depuis le début : la commande suivante supprime les topics, les offsets et les bases SQLite (l'option `--profile tools` inclut le volume des bases). Elle ne modifie pas vos fichiers.

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
| `exercise-tests` échoue | C'est attendu au départ de l'atelier 2 ; lire les assertions |
| La connexion SSH se coupe pendant une commande | Se reconnecter ; les données sont conservées. Utiliser `tmux` pour les commandes longues |

Kafka n'expose aucun port sur la VM : les scripts utilisent `kafka:9092` sur le réseau interne de Docker Compose. Le laboratoire fonctionne en clair sur un broker unique ; ce n'est pas un modèle de déploiement de production. Les contrats sont validés dans l'application, sans Schema Registry.

## Recette technique

Pour vérifier l'ensemble de l'environnement en une fois (formateur, ou après une préparation de VM) :

```sh
docker compose run --rm app integration
docker compose run --rm app transactions
```

Résultat attendu : `INTEGRATION OK` puis `TRANSACTIONS OK`. Ces recettes utilisent leurs propres topics et ne modifient pas les données des ateliers. Le workflow `.github/workflows/verify.yml` exécute la même recette sur GitHub Actions à chaque modification du dépôt.
