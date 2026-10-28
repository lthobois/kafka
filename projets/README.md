# Trois projets de développement sur un socle commun

## Projet 01 — Auditeur de flux

Responsabilité : comprendre un système existant avant de le modifier. Entrées : `data/legacy.json`, `data/legacy-topology.json`. Application : `src/bus.ts`, commandes `seed`, `read` et outils Kafka. Résultat attendu : expliquer pourquoi le même groupe ne convient pas à deux services devant lire tous les événements. Vérification : deux groupes distincts lisent chacun le jeu complet.

Défi après l'atelier : ajouter à la sortie de `readRecords` le nom du groupe et le temps écoulé, sans modifier les offsets. Rejouer avec deux groupes pour rendre les traces plus faciles à interpréter.

## Projet 02 — Producteur de commandes

Responsabilité : publier un fait métier stable avec un contrat contrôlé. Fichiers à modifier : `exercises/order-created.v1.json`, `exercises/routing.ts`. Le transport est fourni dans `src/bus.ts` pour se concentrer sur le design. Résultat attendu : tous les `exercise-tests` passent ; les records portent `order_id` comme clé.

Défi après l'atelier : concevoir `OrderCancelled` dans un nouveau fichier de contrat et un nouveau topic. Choisir si l'ordre entre création et annulation est indispensable ; comparer alors un topic commun par agrégat à deux topics par type de fait. Ajouter un test de l'ancien lecteur avant d'étendre l'enveloppe.

## Projet 03 — Préparation logistique

Responsabilité : produire un seul effet SQLite par événement, même après rejeu. Fichiers : `src/worker.ts`, `src/domain.ts`. Modes `naive` et `safe` permettent de comparer les comportements. Résultat attendu : deux expéditions pour les deux faits métier valides du jeu de résilience, une déduplication et deux quarantaines. Vérification : métriques, base et tests.

Défi après l'atelier : ajouter un compteur `failed_permanent` sans absorber les exceptions Kafka ou SQLite ; ajouter un test qui distingue une panne de stockage d'une erreur de donnée. Le corrigé de référence est le comportement existant : ces pannes arrêtent le worker et laissent l'offset non validé. Un autre défi consiste à simuler la panne de publication DLQ et à vérifier que le commit n'a pas lieu.

Les extensions ne sont pas requises pour réussir la journée. La boucle séquentielle, les commits par record et le retry bloquant privilégient la compréhension. Pour un service à fort débit, il faut étudier lots, gestion des partitions, pauses, délais de poll, résilience du stockage et ordre métier avant d'optimiser.
