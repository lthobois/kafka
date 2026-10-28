# Évaluation finale — Kafka développeurs et responsables d'application

Sur la VM, créer le fichier `reponses-evaluation.txt` dans `~/kafka` (`nano reponses-evaluation.txt`). Copier les numéros 1 à 10, répondre en une phrase par question puis enregistrer. Comparer ensuite au corrigé ci-dessous ; ne pas éditer la page Git. Aucune restitution orale n'est requise.

## Questions

1. Shipping et analytics doivent chacun lire toutes les commandes. Comment choisir leurs groupes ?
2. Un topic à trois partitions garantit-il l'ordre de tous ses événements ?
3. Pourquoi utiliser `order_id` comme clé et conserver aussi `event_id` ?
4. Un ancien lecteur ignore les champs inconnus. Comment tester l'ajout facultatif de `channel` ?
5. Où placer le commit dans le consommateur du laboratoire, et quel risque demeure ?
6. Une quantité négative et un timeout d'entrepôt suivent-ils le même chemin ?
7. Quel risque d'ordre apporte le transfert d'un événement vers un retry topic ?
8. Le lag est nul, mais la DLQ augmente. Peut-on déclarer le flux métier sain ?
9. Avant un reset d'offsets, quels éléments contrôler ?
10. Quelles informations un responsable d'application doit-il fournir pour le flux clients, même avec un Kafka managé ?

## Corrigé expliqué

1. **Deux groupes distincts.** Les instances d'un même traitement peuvent partager leur groupe ; deux traitements indépendants ne doivent pas se répartir les événements destinés à chacun. Référence atelier 1.
2. **Non, l'ordre est par partition.** Une clé stable aide à regrouper les faits d'un agrégat ; changer le nombre de partitions demande une stratégie d'évolution. Référence atelier 5.
3. **La clé représente la commande ; l'identité représente le fait.** L'une sert au partitionnement, l'autre à la déduplication et à la traçabilité. Référence atelier 2.
4. **Tester l'ancien message sans `channel` et le nouveau avec `channel`, avec les lecteurs concernés.** Le test du laboratoire ne prouve pas la compatibilité de tous les consommateurs. Référence atelier 2.
5. **Après l'effet réussi ou la DLQ confirmée.** Une panne avant le commit peut conduire à une relecture ; l'effet doit être idempotent. Référence atelier 4.
6. **Non.** La donnée invalide est une erreur permanente ; le timeout peut être transitoire et supporter un retry borné. Une panne Kafka ou SQLite ne doit pas être validée comme erreur de donnée. Référence atelier 4.
7. **Un fait suivant peut réussir avant le fait en reprise.** Choisir une règle de version métier, de rejet des données obsolètes ou de blocage approprié. Référence atelier 7.
8. **Non.** Les positions ont avancé, mais certains effets métier ont échoué. Contrôler délai métier, résultats et quarantaines. Référence atelier 6.
9. **Groupe inactif, périmètre, données encore conservées, cause corrigée et effets déjà réalisés.** Commencer par un plan ; un reset ne supprime rien et ne restaure pas la rétention expirée. Référence atelier 6.
10. **Sens et source du fait, clé, version métier, attributs nécessaires, destinataires, délai, owner, alerte et reprise.** Le service managé ne définit pas ces engagements applicatifs. Références ateliers 3 et 6.

Repère d'autopositionnement : huit réponses correctes sur dix, en corrigeant toute erreur sur 5, 6, 7 ou 9 avant réutilisation sur un projet réel. Ce repère n'est pas une certification. Les preuves exécutables des ateliers complètent le quiz.
