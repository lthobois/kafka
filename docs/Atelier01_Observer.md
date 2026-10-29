# Atelier 1 — Lire un journal avec deux groupes

La billetterie doit diffuser les ventes à plusieurs services. Objectif : constater que la lecture ne supprime pas les événements et que chaque groupe conserve sa position. Le flux legacy est un contre-exemple déjà fourni ; son diagnostic est expliqué directement.

Prérequis : étapes du [README](../README.md) terminées, Kafka sain et `app init` exécuté. Environnement : VM Linux en SSH, Docker, Kafka 4.1.2 et application Node.js fournie. Aucune modification de code. Toutes les commandes partent de `~/kafka`, dans un seul terminal. Les références **A1-thématique.action.tâche** permettent de signaler un blocage.

## 1. Lire les mêmes événements avec deux services

### 1.1. Publier les quatre records fournis dans legacy-events

- **A1-1.1.1** Publier une seule fois le jeu fourni. Il mélange volontairement ventes et traces techniques pour illustrer un flux mal conçu.


<!-- compose-pedagogie -->
**Avant de lancer :**

`run --rm app` crée un conteneur ponctuel puis le retire à la fin ; les volumes nommés restent présents. Le point d’entrée de [compose.yaml](../compose.yaml) est `node src/cli.ts` : les mots après `app` sont les arguments de ce programme.

`seed legacy` lit les quatre objets de [data/legacy.json](../data/legacy.json) et les publie, sans clé, dans `legacy-events`. Relancer la commande ajoute quatre records ; elle ne remplace pas les précédents.
**Code à lire — [src/cli.ts](../src/cli.ts#L88)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/cli.ts:88-89 -->
```ts
for (const event of JSON.parse(readFileSync(resolve(ROOT, 'data/legacy.json'), 'utf8')))
  await send(p, 'legacy-events', null, event);
```


```sh
cd ~/kafka
docker compose run --rm app seed legacy
```

- **A1-1.1.2** Vérifier `SEEDED legacy`. Diagnostic fourni : environnements mélangés, montants ambigus, absence d'identité métier stable. Les ateliers suivants utilisent un contrat explicite pour éviter ces problèmes.

### 1.2. Lire le journal avec audit-A1 puis analytics-A1

- **A1-1.2.1** Exécuter les deux lectures. Chaque groupe représente ici un service indépendant.


<!-- compose-pedagogie -->
**Avant de lancer :**

**Commande 1 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `legacy-events` avec le groupe `audit-A1`, afficher les records et valider leur offset suivant. `--count 4` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

Cette lecture change les positions du groupe, pas la base SQLite. Un groupe déjà utilisé reprend sa position ; `fromBeginning: true` ne force pas le retour au début s’il possède un offset valide.
**Code à lire — [src/bus.ts](../src/bus.ts#L67)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:67-68 -->
```ts
const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
  fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
```


**Commande 2 du bloc.** Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `legacy-events` avec le groupe `analytics-A1`, afficher les records et valider leur offset suivant. `--count 4` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 15` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.
**Code à lire — [src/bus.ts](../src/bus.ts#L92)** : extrait de la version fournie, à comprendre sans le retaper.

<!-- code-source: src/bus.ts:92-93 -->
```ts
await c.commitOffsets([{ topic: payload.topic, partition: payload.partition,
  offset: (BigInt(payload.message.offset) + 1n).toString() }]);
```


```sh
docker compose run --rm app read --topic legacy-events --group audit-A1 --count 4
docker compose run --rm app read --topic legacy-events --group analytics-A1 --count 4
```

- **A1-1.2.2** Vérifier `READ records=4` deux fois et comparer les couples partition/offset. Pourquoi les deux groupes retrouvent-ils les événements ? **Réponse :** chacun mémorise sa progression ; la première lecture n'a pas retiré les records du journal.

## 2. Observer la reprise d'un groupe

### 2.1. Relancer audit-A1 sans republier le jeu

- **A1-2.1.1** Relancer le groupe déjà passé. Cette lecture attend volontairement cinq secondes.


<!-- compose-pedagogie -->
**Avant de lancer :**

Même lanceur : une nouvelle exécution de `app` via `node src/cli.ts`, sans vider les données persistantes.

`read` appelle [readRecords et consume](../src/bus.ts) : lire `legacy-events` avec le groupe `audit-A1`, afficher les records et valider leur offset suivant. `--count 4` est le nombre recherché ; la lecture termine aussi à l’expiration de `--timeout 5` secondes. Elle peut donc afficher moins de records si ce groupe n’a plus rien à lire.

```sh
docker compose run --rm app read --topic legacy-events --group audit-A1 --count 4 --timeout 5
```

- **A1-2.1.2** Vérifier `READ records=0`. Est-ce une panne ? **Réponse : non.** Le groupe a déjà validé les quatre positions ; aucun nouvel événement n'a été publié. Un autre groupe pourrait relire le journal conservé.

## 3. Vérifier le résultat

### 3.1. Comparer les trois résultats aux références

- **A1-3.1.1** Contrôler : quatre records pour chacun des deux groupes, puis zéro lors de la reprise d'audit-A1. À retenir : un groupe par traitement indépendant, offset propre à chaque partition, conservation indépendante de la lecture.
- **A1-3.1.2** Si un résultat diffère, vérifier le nom du groupe et l'absence de double publication. `Unknown topic` signifie que `app init` manque. Ne pas nettoyer les volumes pour corriger un simple nom de groupe. Continuer avec l'[atelier 2](Atelier02_Concevoir.md).
