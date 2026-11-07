# Critère 2 — identification des RWA

Revue initiale du 7 novembre 2026 (Europe/Paris). Le registre est volontairement
non exhaustif. Il alimente le filtre RWA du tableau de bord IBC.

## Règle d'identification

Un actif est retenu dans le périmètre strict uniquement après rapprochement :
1. **Produit économique** : documentation primaire de l'émetteur décrivant le
   droit représenté et les actifs sous-jacents annoncés. Une marque, un ticker
   ou l'étiquette marketing « RWA » ne constituent pas une identité.
2. **Identité native** : chaîne exacte, dénomination ou contrat exact, unités,
   source de l'émetteur et métadonnées techniques corroborantes.
3. **Représentation IBC** : trace exacte et hash de la dénomination locale,
   canaux et contreparties issus du registre épinglé à un commit ; pour les
   paquets historiques observés, canal OPEN et client Tendermint consultés à
   la hauteur exacte du paquet, identifiant la chaîne d'origine attendue.
4. **Traçabilité** : dates de revue et d'observation, URL des sources, hash des
   documents consultés, snapshots techniques conservés et réponses historiques.

L'identification est une revue réalisée aujourd'hui sur des paquets historiques.
Elle ne certifie pas rétroactivement les réserves, l'éligibilité de chaque
porteur ou le statut commercial du réseau. La nature et la couverture réelles
des actifs hors chaîne exigent des attestations et documents supplémentaires.
Les pages produits ne sont pas un audit indépendant des réserves. Le statut
actuel de mint/redeem et du support réseau reste explicitement non vérifié.

## Premier registre

| Produit | Catégorie choisie | Identité retenue | Inclusion RWA stricte |
|---|---|---|---|
| USDY — Ondo Finance | Produit d'investissement tokenisé | Noble `noble-1`, `ausdy`, 18 décimales ; représentations directes IBC observées sur Osmosis et Injective | Oui, si identité et preuve historique concordent |
| USYC — Circle / USYC | Produit de fonds tokenisé | Produit documenté ; identité mainnet Noble encore à établir | Non, candidat uniquement |
| USDN — Noble / M0 | Stablecoin à rendement | Noble `uusdn`, 6 décimales | Non, catégorie séparée |
| USDC — Circle | Stablecoin fiat | Noble `uusdc`, 6 décimales | Non, catégorie séparée |

Le choix de séparer les stablecoins est une convention de mesure explicite ;
il ne signifie pas qu'ils n'ont aucun adossement à des actifs réels. Les tokens
de gouvernance de plateformes RWA ne sont pas assimilés à leurs produits.
Les wrappers, actifs synthétiques, versions bridgées et rebasing demandent une
preuve spécifique : pas de fusion sur le seul nom et pas de conversion implicite.
Un actif absent de cette liste est **non revu**, jamais automatiquement « non RWA ».

## Ce que montrent les données déjà collectées

Pour la cohorte du **5 novembre 2026**, l'identité USDY est établie sur :

| Chaîne | Observations sortantes | Observations entrantes |
|---|---:|---:|
| Noble | 3 | 2 |
| Osmosis | 1 | 1 |
| Injective | 1 | 2 |

Ces **5 émissions et 5 réceptions ne sont pas 10 mouvements économiques uniques**.
Un même transfert peut être observé des deux côtés. Cet audit identifie des
paquets ; il ne déclare ni leur succès ni un volume économique, un encours ou un
transit via le Hub. Les cas absents ne prouvent pas une absence globale de RWA.
L'index du fournisseur reste une limite de couverture.

Les canaux observés sont `channel-148` côté Injective ↔ `channel-31` côté Noble,
et `channel-750` côté Osmosis ↔ `channel-1` côté Noble. Leurs réponses historiques
sont conservées aux cinq hauteurs distinctes observées. Aucune valeur actuelle
ne remplace une réponse historique indisponible.

## Fichiers et exécution

- `config/criterion2_rwa_registry.json` : produits, catégories, représentations,
  sources requises et règles d'inclusion ; revue manuelle, pas d'admission par ticker.
- `data/criterion2_rwa_registry/2026-11-07/` : manifeste des sources, snapshots
  compressés du Chain Registry et réponses historiques canal/client.
- `criterion2_rwa_registry.py` : validation des preuves et audit hors ligne.
- `criterion2_rwa_identification.csv` : observations de produits RWA examinés,
  identité, état d'inclusion et hash de preuve ; stablecoins exclus de ce CSV.
- `criterion2_rwa_identification_summary.json` : couverture de l'audit et
  compteurs séparés des produits RWA, stablecoins et observations non revues.

Lancer `python criterion2_rwa_registry.py` pour reproduire cet audit depuis les
checkpoints et preuves existants. Aucune requête réseau n'est nécessaire.
Une représentation connue sans preuve historique à la hauteur du paquet reste
`pending_historical_route`, exclue du périmètre strict. Une preuve contradictoire
ou une ambiguïté provoque une erreur, sans classification de remplacement.

Le registre initial épingle le Chain Registry au commit
`d6d22e51f4b520ed993f3a939ab64d7979e2fcd3`. Les descriptions, unités, canaux
et identités doivent être revus avant d'ajouter un produit ou une représentation.
Une nouvelle chaîne, un nouveau canal ou un chemin multi-saut ne sont pas admis
par analogie avec ceux déjà validés. Cet audit initial est daté et n'est pas
encore exécuté automatiquement par les workflows économiques.

Le panneau **Filtre RWA vérifiés** présente les montants confirmés en unités
du produit, le solde lorsqu'une borne commune de confirmations est disponible
et les transits PFM identifiés. Le CSV `ibc_rwa_daily.csv` conserve les montants
exacts. Le workflow vérifie les nouveaux canaux historiques des représentations
déjà inscrites au registre. Une preuve absente ou contradictoire laisse
l'identité en attente.
Les encours demandent un collecteur de soldes/supply et une déduplication des
escrows et représentations, distincts de cet audit de flux.

## Sources primaires

- [USDY et identifiant Noble publiés par Ondo](https://ondo.finance/usdy).
- [Documentation USDY](https://docs.ondo.finance/general-access-products/usdy/basics).
- [USYC : produit et structure annoncés](https://usyc.docs.hashnote.com/overview/introduction).
- [USDN : description par M0](https://www.m0.org/press-releases/noble-launches-usdn-a-programmatic-and-composable-yield-stablecoin-for-application-developers).
- [USDC : description par Circle](https://www.circle.com/usdc).
- [Métadonnées Noble épinglées](https://github.com/cosmos/chain-registry/blob/d6d22e51f4b520ed993f3a939ab64d7979e2fcd3/noble/assetlist.json).
- [Canaux Injective–Noble épinglés](https://github.com/cosmos/chain-registry/blob/d6d22e51f4b520ed993f3a939ab64d7979e2fcd3/_IBC/injective-noble.json).
- [Canaux Noble–Osmosis épinglés](https://github.com/cosmos/chain-registry/blob/d6d22e51f4b520ed993f3a939ab64d7979e2fcd3/_IBC/noble-osmosis.json).
