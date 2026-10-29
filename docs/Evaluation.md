# Synthèse guidée — six repères de la journée

Chaque question est suivie de sa réponse. Comparer aux résultats des quatre ateliers ; aucun écrit ni score n'est demandé.

## 1. Pourquoi deux groupes pour PDF et statistiques ?

**Réponse :** chacun doit lire toutes les ventes. Deux instances du même traitement peuvent partager un groupe ; deux traitements indépendants utilisent des groupes distincts. LAB-01 montre deux lectures complètes, puis la reprise d'un groupe déjà passé.

## 2. La clé garantit-elle un ordre global ?

**Réponse : non.** L'ordre est propre à une partition. La clé ticketId rapproche les faits d'un billet ; la clé concert eventId concentrerait une ouverture de vente sur une partition. LAB-02 fournit la clé adaptée.

## 3. Le contrat et l'ACK prouvent-ils que le PDF est produit ?

**Réponse : non.** Le contrat vérifie la forme et les contraintes connues ; l'ACK confirme la publication selon les garanties du cluster. Le résultat métier exige une preuve du consommateur. Le laboratoire simule cette preuve dans SQLite.

## 4. Pourquoi le mode sûr garde-t-il deux effets après rejeu ?

**Réponse :** le marqueur event_id et l'effet sont validés ensemble en SQLite. Un fait déjà traité retrouve son marqueur. L'offset est validé après l'effet ou après confirmation de la DLQ. Un email ou une API externe exige son propre mécanisme d'idempotence.

## 5. Que prouvent retry, DLQ et lag ?

**Réponse :** le retry borné réessaie une panne transitoire ; la DLQ isole un échec ; le lag mesure des positions non validées. Un lag nul peut accompagner une DLQ non vide. Vérifier aussi le résultat métier et son délai.

## 6. Que vérifier avant de rejouer ?

**Réponse :** cause corrigée, périmètre limité, groupe inactif si reset, données encore conservées et effets déjà réalisés identifiés. LAB-06 affiche un plan dry-run ; il ne déplace aucun offset et n'annule aucun effet.
