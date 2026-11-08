# Cosmos Hub Monitor — Référentiel 2026

L’observatoire vérifie si le Cosmos Hub progresse vers une création de valeur
économique durable pour ATOM. Il distingue l’atteinte des jalons de 2026 et
la valeur effectivement captée par ATOM.

| Critère officiel | Condition de validation |
| --- | --- |
| 1 — Revenus opérationnels du Cosmos Hub | Au moins une source de revenus du Hub est opérationnelle et produit des revenus mesurables. |
| 2 — Refonte de la tokenomics d’ATOM | Une nouvelle tokenomics est effectivement adoptée et déployée on-chain. |
| 3 — Inflation d’ATOM inférieure à 5 % | Le taux d’inflation annuel effectif vérifié on-chain est strictement inférieur à 5 %. |
| 4 — Extension d’IBC Eureka au-delà d’Ethereum | IBC Eureka est effectivement opérationnel sur un autre réseau ou environnement. |
| 5 — Revenus commerciaux d’une entreprise utilisant le Hub | Au moins une entreprise paie pour un service du Cosmos Hub, avec un mécanisme clair de transmission de valeur à ATOM. |

Le tableau de bord actuel publie les mesures du critère 1. Chaque critère
doit pouvoir être évalué comme validé, non validé ou données insuffisantes,
avec les preuves associées.

## Mesures du critère 1

| Sous-critère | Mesures et limites | Pipeline |
| --- | --- | --- |
| **1A — Activité IBC** | Transactions du Hub, transactions IBC et leur proportion sur les journées UTC dont tous les blocs sont couverts. | [Économie du Hub](run_criterion1_economy_pipeline.py) |
| **1B — Centralité des flux IBC** | Flux sortants et part relative du Hub face à Osmosis et Noble, avec Injective dans le périmètre actuel. Comparaison sur les mêmes journées UTC. La part USD porte sur le sous-ensemble valorisé quand des prix manquent. Les entrées, règlements, flux nets, transits et encours RWA complètent ce suivi. | [Flux IBC multichaînes](run_criteria1b_pipeline.py) |
| **1C — Revenus** | Frais du Hub et frais IBC collectés, avec audit des preuves de paiement. L’attribution d’une recette supplémentaire exige un paiement vérifiable, son origine et sa destination. | [Économie du Hub](run_criterion1_economy_pipeline.py) |
| **1D — Émission et couverture économique** | Inflation horodatée, variation de l’offre et comparaison des frais avec l’émission sur les mêmes blocs. L’audit réconcilie les créations et destructions d’ATOM avec l’offre et alimente le taux du critère 3. [Méthode de l’audit](EMISSION.md) | [Économie du Hub](run_criterion1_economy_pipeline.py) |

## Ventilation Eureka du sous-critère 1C

Le workflow Économie publie les frais payés au Hub par les transactions réussies
associées à la route Eureka Ethereum. Le collecteur parcourt les événements de
paquets indexés, vérifie le couple de clients du Hub et d’Ethereum, puis
déduplique par identifiant de transaction. Les preuves de frais existantes
servent au calcul. Si une preuve manque, le total de la journée reste indisponible.
La part utilise les frais du Hub vérifiés sur les mêmes blocs.

Le CSV `hub_criterion1_eureka_daily.csv` couvre le sous-ensemble identifiable
dans l’index RPC. Une transaction mixte est comptée une seule fois avec ses
frais complets. Les frais de relais sont hors du périmètre. La route initiale
est définie dans le [registre de déploiement Eureka](https://github.com/cosmos/eureka-ops/blob/main/deployments/mainnet/1.json).

## Versements au fonds communautaire

La page `community-pool.html` présente les transferts explicites vers le fonds,
tous actifs confondus, avec transaction et attribution. Le registre dépend de
l’index RPC et affiche ses bornes. Les crédits automatiques restent hors de son
périmètre. Le workflow Économie actualise le relevé et conserve le dernier
instantané si le fournisseur ne reproduit pas les versements déjà vérifiés.

## Pistes de suivi reportées

- **1B** — Veille des nouvelles émissions RWA et des premiers passages par Cosmos Hub.
- **1C** — Vérifier les versements liés à l’accord Injective USDC, puis documenter les éventuels rachats d’ATOM et leur transfert au fonds communautaire.

## Noms et compatibilité

Les workflows GitHub Actions se présentent sous les noms
**Criterion 1A, 1C, 1D — Hub economy** et **Criterion 1B — Multichain IBC flows**.
Leurs fichiers `.github/workflows/daily.yml` et `.github/workflows/daily_criteria2.yml`
gardent leurs chemins pour la reprise et l’historique des exécutions.
Le bilan par mail écoute les nouveaux noms.

Les anciens points d’entrée Python redirigent vers les commandes ci-dessus.
Les liens publics `criterion1.html` et `criterion2.html`, leurs versions
anglaises et les identifiants historiques des fichiers de données sont conservés.
Dans ces chemins hérités, `criterion2` désigne le sous-critère **1B**.

Voir la [collecte économique](README.md), la [méthode 1B](CRITERION2.md)
et le [guide du tableau de bord](DASHBOARD.md).
