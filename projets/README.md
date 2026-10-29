# Trois projets guidés sur un socle commun

## Projet 01 — Auditeur de flux

Entrées : `data/legacy.json` et `data/legacy-topology.json`. Les commandes `seed`, `read` et les outils Kafka montrent le journal et les groupes. Pourquoi deux services utilisent-ils des groupes distincts ? **Réponse :** chaque groupe lit tous les événements ; un groupe commun répartirait les partitions entre les deux services. Vérification : les deux lectures de l'atelier 1 reçoivent chacune les quatre records.

## Projet 02 — Producteur de ventes

Le contrat `contracts/ticket-sold.v1.json` et la clé `solutions/routing.ts` sont fournis. L'atelier 2 les sélectionne directement avec `--solution`, publie trois billets puis observe leur lecture. Pourquoi `ticketId` plutôt que `eventId` ? **Réponse :** la clé billet répartit les ventes d'un même concert et garde une affinité par billet ; la clé concert concentrerait ce pic sur une seule partition.

## Projet 03 — Génération PDF simulée

Lire `src/worker.ts` et `src/domain.ts`. Les modes `naive` et `safe` comparent un effet SQLite avec ou sans déduplication. Résultat attendu : trois effets en mode naïf, deux en mode sûr, puis aucun effet supplémentaire lors du rejeu sûr. La base représente le traitement du PDF ; elle ne produit pas de document binaire. Pourquoi une panne SQLite arrête-t-elle le worker ? **Réponse :** il faut conserver l'offset non validé pour reprendre après rétablissement, plutôt que classer tous les événements comme invalides.

Les manipulations et les réponses sont dans les fiches. Aucun défi de rédaction, de conception libre ou de développement supplémentaire n'est demandé.
