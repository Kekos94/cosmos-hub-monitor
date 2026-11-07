# 1D — Mesurer l’émission d’ATOM

L’audit porte sur les journées UTC dont le collecteur a vérifié les bornes et
la couverture complète. Il interroge les résultats de tous les blocs de la
journée, y compris les blocs vides. Il confronte les événements `mint` du
module monétaire à ses crédits bancaires `coinbase`, additionne toutes les
créations de `uatom` et relève les destructions `burn`. Les créations et
destructions sont additionnées en unités entières. Les événements de
finalisation et ceux des transactions réussies sont lus séparément.

À chaque journée, l’offre du module bancaire est lue à deux hauteurs
historiques vérifiées, le bloc précédant la journée et son dernier bloc.
L’audit est réconcilié si créations totales moins destructions égale
exactement offre finale moins offre initiale. Les frais restent une mesure
distincte. Un jour incomplet ou un écart d’offre bloque la publication de
l’émission et des taux pour ce jour.

| Indicateur | Calcul |
| --- | --- |
| Émission monétaire mesurée | Somme des créations d’ATOM attribuées au module `mint`. |
| Créations brutes | Somme des créations d’ATOM de tous les modules. |
| Burn effectif | Somme des destructions d’ATOM, en unités du token. |
| Inflation monétaire annualisée | Émission monétaire ÷ offre initiale × secondes de 365 jours ÷ durée réelle entre les deux blocs × 100. |
| Croissance nette annualisée de l’offre | Même calcul en utilisant la variation nette de l’offre. |
| Inflation monétaire sur 365 jours | Somme des émissions monétaires sur 365 journées UTC consécutives réconciliées ÷ offre initiale de la fenêtre × 100. |

Le **critère 3** utilise la dernière mesure sur 365 jours, avec comparaison
stricte au seuil de 5 %. Une projection journalière renseigne sur le rythme
récent. Le taux configuré lu par le collecteur historique demeure une
observation du protocole. Les trois séries ont des interprétations distinctes.

Le CSV `hub_criterion1_emission_daily.csv` publie les hauteurs, la couverture,
l’offre, les montants, l’écart de rapprochement et les taux disponibles. Les
points de reprise contiennent les montants par bloc, les fournisseurs et les
dates d’observation. Ils sont conservés par journée dans
`data/criterion1_emission/`. Les séries historiques estimées restent
accessibles séparément.

Le workflow Économie exécute l’audit après la collecte des blocs. Une reprise
peut continuer après le dernier lot sauvegardé. Pour auditer une journée déjà
collectée manuellement ou reconstruire le CSV hors ligne :

```sh
python criterion1_emission.py --collect 900 --day 2026-11-06
python criterion1_emission.py
python build_web_dashboard.py
```

Références techniques : [module mint](https://github.com/cosmos/cosmos-sdk/tree/v0.53.8/x/mint)
et [événements bancaires](https://github.com/cosmos/cosmos-sdk/tree/v0.53.8/x/bank#keeper-events).
