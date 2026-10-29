# Atelier 4 — Éviter les doublons et isoler les erreurs

Objectif : comparer un traitement naïf à un traitement idempotent, observer la DLQ puis rejouer sans produire un second effet. SQLite simule la prise en charge du PDF ; aucun document binaire n'est généré.

Prérequis : environnement du [README](../README.md) opérationnel avec `app init`. Même VM Linux, Kafka 4.1.2 et application Node.js fournie. Un terminal, aucune modification de code. Publier le jeu une seule fois dans un environnement neuf. Les références **A4-thématique.action.tâche** identifient les tâches.

## 1. Comparer deux traitements du même journal

### 1.1. Publier les cinq records puis exécuter le mode naïf

- **A4-1.1.1** Publier le jeu isolé. Il contient un fait valide, son doublon, une panne transitoire qui finit par réussir, une donnée invalide et une panne persistante.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** `run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`seed resilience` publie cinq records dans `dev.lab.resilience.v1` : le premier fait deux fois, un fait avec deux échecs simulés, une quantité invalide et une panne persistante. Le header `lab-failures` commande la simulation ; le broker n’injecte pas ces erreurs lui-même.
**Code à lire — [src/cli.ts](../src/cli.ts#L100)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:100-103 -->
```ts
const events = [[makeEvent(1), 0], [makeEvent(1), 0], [makeEvent(2), 2], [makeEvent(3), 0], [makeEvent(4), 9]] as const;
events[3][0].data.quantity = -1;
for (const [event, failures] of events)
  await send(p, flags.topic ?? MAIN, event.data.ticketId, event, { 'lab-failures': String(failures) });
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`worker` est une commande propre au laboratoire. [src/cli.ts](../src/cli.ts) transmet les paramètres à [runWorker](../src/worker.ts) : lire `dev.lab.resilience.v1` avec `naive-A4`, traiter 5 records et utiliser `/state/naive-A4.db` pour l’état métier. `--dlq-topic` désigne le topic de quarantaine. Les deux topics doivent déjà exister.

`--mode naive` saute la recherche du marqueur seen dans [apply](../src/domain.ts) ; chaque fait valide insère un effet dans tickets_pdf, même s’il a déjà été reçu. Ce mode garde cependant la validation, les retries et la DLQ pour rendre la comparaison équitable.

Dans cet extrait, `db.exec(COMMIT)` valide la transaction SQLite. Le commit des offsets Kafka est une autre opération, appelée ensuite par runWorker : les deux étapes ne sont pas une transaction distribuée.

Le worker valide l’événement, réessaie une erreur transitoire au plus trois tentatives, puis attend l’ACK d’un éventuel envoi DLQ avant [commit](../src/bus.ts). Les métriques décrivent cette exécution ; le compteur SQLite décrit les effets conservés.
**Code à lire — [src/domain.ts](../src/domain.ts#L62)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/domain.ts:62-64 -->
```ts
db.prepare('INSERT INTO tickets_pdf(event_id, ticketId, quantity) VALUES (?, ?, ?)')
  .run(event.event_id, event.data.ticketId, event.data.quantity);
db.exec('COMMIT');
```


**Commande 3 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`--no-deps` évite de démarrer Kafka pour ce contrôle local.

`status --db /state/naive-A4.db` ouvre cette base en lecture seule et affiche tickets_pdf. C’est une preuve métier locale : aucun message n’est lu et aucun offset n’est déplacé. Le volume `app-state:/state` permet de retrouver la base même si le conteneur du worker a été supprimé.
**Code à lire — [src/cli.ts](../src/cli.ts#L33)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:33-37 -->
```ts
const db = new DatabaseSync(flags.db ?? '/state/pdf.db', { readOnly: true });
try {
  console.log('tickets_pdf=' + db.prepare('SELECT COUNT(*) AS n FROM tickets_pdf').get()!.n);
  console.table(db.prepare('SELECT event_id, ticketId, quantity FROM tickets_pdf ORDER BY id').all());
} finally { db.close(); }
```


```sh
cd ~/kafka
docker compose run --rm app seed resilience --topic dev.lab.resilience.v1
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group naive-A4 --db /state/naive-A4.db --mode naive --count 5
docker compose run --rm --no-deps app status --db /state/naive-A4.db
```

- **A4-1.1.2** Vérifier cinq records, `accepted=3`, `duplicate=0`, `retry=4`, `dlq=2` et `tickets_pdf=3`. Pourquoi deux effets pour `evt-001` ? **Réponse :** deux publications applicatives sont deux records ; l'idempotence du producer ne remplace pas la déduplication métier.

### 1.2. Lire le même journal avec le mode sûr et une base distincte

- **A4-1.2.1** Utiliser un autre groupe et une autre base pour comparer équitablement.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`worker` est une commande propre au laboratoire. [src/cli.ts](../src/cli.ts) transmet les paramètres à [runWorker](../src/worker.ts) : lire `dev.lab.resilience.v1` avec `safe-A4`, traiter 5 records et utiliser `/state/safe-A4.db` pour l’état métier. `--dlq-topic` désigne le topic de quarantaine. Les deux topics doivent déjà exister.

Sans `--mode`, [apply](../src/domain.ts) utilise `safe`. Le programme recherche event_id dans seen, absorbe un doublon de contenu identique et enregistre marqueur et nouvel effet dans une même transaction. `--db` reste un fichier persistant : changer seulement `--group` ne le vide pas.
**Code à lire — [src/domain.ts](../src/domain.ts#L53)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/domain.ts:53-61 -->
```ts
if (mode === 'safe') {
  const row = db.prepare('SELECT digest FROM seen WHERE event_id=?').get(event.event_id);
  if (row) {
    if (row.digest !== digest) throw new PermanentError('event_id réutilisé avec un contenu métier différent');
    db.exec('COMMIT');
    return 'duplicate';
  }
  db.prepare('INSERT INTO seen VALUES (?, ?)').run(event.event_id, digest);
}
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`--no-deps` évite de démarrer Kafka pour ce contrôle local.

`status --db /state/safe-A4.db` ouvre cette base en lecture seule et affiche tickets_pdf. C’est une preuve métier locale : aucun message n’est lu et aucun offset n’est déplacé. Le volume `app-state:/state` permet de retrouver la base même si le conteneur du worker a été supprimé.

```sh
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group safe-A4 --db /state/safe-A4.db --count 5
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A4-1.2.2** Vérifier `accepted=2`, `duplicate=1`, `retry=4`, `dlq=2` et `tickets_pdf=2`. La transaction SQLite enregistre ensemble effet et marqueur `event_id` ; une relecture retrouve le marqueur. Les quatre retries sont les tentatives supplémentaires de deux événements, pas quatre faits métier.

## 2. Examiner les erreurs

### 2.1. Lire les quatre quarantaines des deux modes

- **A4-2.1.1** Lire les résultats des deux expériences avant le rejeu.


<!-- compose-pedagogie -->
**Avant de lancer :**

Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `dev.lab.resilience.dlq.v1` avec le groupe `audit-dlq-A4`, afficher les records et valider leur offset suivant. `--count 4` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

Cette lecture change les positions du groupe, pas la base SQLite. Un groupe déjà utilisé reprend sa position ; `fromBeginning: true` ne force pas le retour au début s’il possède un offset valide.
**Code à lire — [src/bus.ts](../src/bus.ts#L67)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:67-68 -->
```ts
const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
  fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
```


```sh
docker compose run --rm app read --topic dev.lab.resilience.dlq.v1 --group audit-dlq-A4 --count 4
```

- **A4-2.1.2** Vérifier deux `PermanentError` et deux `TransientError`, avec les coordonnées source. La donnée invalide ne déclenche aucun essai métier (`attempts=0`) ; la panne persistante atteint trois tentatives. Pourquoi attendre l'ACK DLQ avant le commit ? **Réponse :** conserver le message à sa source si son transfert échoue.

## 3. Rejouer en conservant l'état métier

### 3.1. Relire avec replay-A4 et la base sûre existante

- **A4-3.1.1** Changer seulement le groupe, en conservant la base. Le nouveau groupe relit les records, sans effacer les effets existants.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`worker` est une commande propre au laboratoire. [src/cli.ts](../src/cli.ts) transmet les paramètres à [runWorker](../src/worker.ts) : lire `dev.lab.resilience.v1` avec `replay-A4`, traiter 5 records et utiliser `/state/safe-A4.db` pour l’état métier. `--dlq-topic` désigne le topic de quarantaine. Les deux topics doivent déjà exister.

Sans `--mode`, [apply](../src/domain.ts) utilise `safe`. Le programme recherche event_id dans seen, absorbe un doublon de contenu identique et enregistre marqueur et nouvel effet dans une même transaction. `--db` reste un fichier persistant : changer seulement `--group` ne le vide pas.

**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`--no-deps` évite de démarrer Kafka pour ce contrôle local.

`status --db /state/safe-A4.db` ouvre cette base en lecture seule et affiche tickets_pdf. C’est une preuve métier locale : aucun message n’est lu et aucun offset n’est déplacé. Le volume `app-state:/state` permet de retrouver la base même si le conteneur du worker a été supprimé.

```sh
docker compose run --rm app worker --topic dev.lab.resilience.v1 --dlq-topic dev.lab.resilience.dlq.v1 --group replay-A4 --db /state/safe-A4.db --count 5
docker compose run --rm --no-deps app status --db /state/safe-A4.db
```

- **A4-3.1.2** Vérifier `accepted=0`, `duplicate=3`, `retry=4`, `dlq=2` et toujours `tickets_pdf=2`. La DLQ contient maintenant six records : les deux mêmes échecs ont été observés dans trois expériences. Ce ne sont pas six ventes invalides distinctes.

## 4. Vérifier les limites

### 4.1. Comparer les résultats à la frontière transactionnelle

- **A4-4.1.1** Contrôler la progression 3 effets naïfs, 2 effets sûrs, puis aucun effet supplémentaire au rejeu. Un email, une API assureur ou un paiement ne partage pas la transaction SQLite : chacun exige son mécanisme d'idempotence.
- **A4-4.1.2** Si les comptes diffèrent, vérifier publications, groupes et chemins de base. Conserver la base sûre pour l'[atelier 6](Atelier06_Responsables-application.md). Ne pas supprimer les volumes entre ces deux fiches.
