# Cosmos Hub Monitor

Le [référentiel 2026](REFERENTIEL.md) définit les cinq critères et rattache les deux pipelines actuels au critère 1.

## Collecte de journées UTC complètes

Le pipeline cible désormais **tous les blocs d'une journée UTC terminée**, et
non les 60 derniers blocs. Une journée contient les blocs dont le timestamp
est dans `[00:00 UTC, minuit suivant[`. Le bloc exactement au premier minuit
est inclus ; celui au minuit suivant appartient à la journée suivante.

Le collecteur recherche les hauteurs par timestamps, puis vérifie les blocs
immédiatement avant et après la journée. Il exige un bloc finalisé à partir
du minuit suivant. Si l'archive nécessaire n'est pas disponible, il échoue
sans transformer les blocs accessibles en total journalier.

Python 3.11+ :

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python run_criterion1_economy_pipeline.py
python generate_dashboard_html.py
```

Les scripts utilisent le dossier du projet, quel que soit le dossier courant.
Les tests utilisent des réponses simulées et n'ont pas besoin du réseau.
La collecte réelle utilise les nœuds RPC/LCD publics et CoinGecko.

## Reprise, avancement et publication

- Au premier lancement, la première journée visée est **hier en UTC**.
  Pour commencer plus tôt sur un historique vide :
  `START_DATE=2026-11-01 python run_criterion1_economy_pipeline.py`.
  Cela exige des nœuds qui conservent les blocs, transactions et états requis.
- Les mesures et leurs snapshots sont sauvegardés dans
  `data/criterion1_days/YYYY-MM-DD.json` après chaque lot entièrement validé.
  La taille par défaut est **120 blocs par lot**, sans plafond de 120 blocs
  pour la journée ou pour l'ensemble de l'exécution.
- La collecte reprend à la première hauteur manquante de la journée en cours.
  Elle termine cette journée avant de passer aux suivantes, sans sauter les
  jours de panne. `START_DATE` ne peut pas déplacer un historique déjà démarré.
- Les métadonnées de **tous** les blocs sont récupérées par plages d'au plus
  20 via `/blockchain`. Chaque hauteur attendue doit être présente une seule fois,
  sur la bonne chaîne. Un bloc vide n'est reconnu qu'avec un compte de transactions
  nul et un engagement de transactions vide.
- Pour les blocs non vides, les données `/block` et les résultats `/block_results`
  sont récupérés par requêtes JSON-RPC groupées d'au plus 10 appels. Les réponses
  sont associées par identifiant, même si le serveur les renvoie dans le désordre.
  Les hauteurs, en-têtes, hashes de blocs et comptes de transactions doivent
  correspondre ; le nombre de résultats doit être égal au nombre de transactions.
- Les types de messages et frais déclarés sont lus localement dans les octets
  Protobuf `TxRaw`, suivant le schéma Cosmos SDK. Les événements exécutés restent
  la source des frais collectés, y compris les transactions échouées. Les attributs
  ABCI en texte ou en base64 sont reconnus. Aucun résultat n'est paginé ni tronqué.
- Si la lecture groupée est indisponible, la lecture individuelle par hash LCD
  reste un secours avec ses contrôles de hauteur/hash. Le paramètre de parallélisme
  ne concerne que ce secours (deux lecteurs dans le workflow). Les réponses
  groupées incohérentes ou tronquées ne sont pas acceptées comme données complètes.
- Les checkpoints sont validés puis écrits par remplacement atomique. Une
  erreur de bloc ou de transaction ne conserve aucun cumul partiel du lot.
  Les lots déjà enregistrés, leurs prix et leurs snapshots restent inchangés.
  Une écriture qui modifierait un ancien snapshot ou le plan enregistré est
  refusée, de même qu'un trou, un chevauchement ou une incohérence temporelle.
- `hub_criterion1_daily_complete.csv` contient **uniquement les journées dont
  tous les blocs et toutes les transactions ont été traités**. Avant la première
  journée complète, il ne contient que son en-tête. Il n'y a aucune estimation
  du total des fees à partir d'un échantillon.
- Le dashboard affiche séparément les journées complètes et l'avancement des
  journées incomplètes : blocs analysés / attendus. Un taux de 100 % désigne la
  couverture des blocs de la journée, pas une preuve de complétude des événements
  de frais. Le statut des fees est indiqué dans une colonne distincte.
- Une journée sans aucun bloc, par exemple durant un arrêt de chaîne, n'est
  déclarée vide qu'après vérification des blocs qui encadrent tout cet intervalle.
  Aucune émission ni dilution n'est inventée pendant cet arrêt.

Le workflow `.github/workflows/daily.yml` est programmé **toutes les 30 minutes**
(aux minutes 17 et 47 de chaque heure UTC), avec un
budget de collecte de 20 minutes vérifié entre les opérations et les lots.
L'exécution suivante reprend automatiquement les mesures enregistrées.
`COLLECT_BUDGET_SECONDS`, `CHUNK_BLOCKS` (1–1000) et `COLLECT_WORKERS` (1–8)
permettent d'adapter les limites.
Les appels RPC/LCD sont espacés d'au moins une seconde par hôte, tous lecteurs
confondus (`HTTP_MIN_INTERVAL_SECONDS`). Les erreurs temporaires (notamment
HTTP 429, 503 et délais réseau) font l'objet de quatre tentatives maximum,
avec attente progressive et respect de `Retry-After` en secondes ou en date
HTTP. Une pause demandée par un hôte s'applique aussi aux autres lecteurs.
Un 404 du fournisseur de secours ne transforme jamais une transaction manquante
en zéro : le fournisseur principal est réessayé s'il a signalé une erreur
temporaire. Si l'attente dépasse le budget restant, le lot est abandonné sans
avancer le checkpoint ; l'exécution suivante reprend au même endroit. Les requêtes déjà en vol et les snapshots
peuvent finir après l'expiration du budget ; le job a une limite de 45 minutes.
La fréquence GitHub Actions n'est pas une garantie d'heure exacte ou de débit.

En cas d'erreur réseau, le workflow valide et commite les lots terminés avant
de signaler l'échec ; le déploiement est bloqué. Une limite de temps normale
produit simplement une journée en cours, jamais un faux total complet.
Les exécutions sont sérialisées pour une même branche et repartent de son
état le plus récent. Les checkpoints, l'export et les copies publiées sont
commités ensemble. La collecte 1B dispose de son propre workflow.

## Périodes des ratios et calculs économiques

Les fees appartiennent exactement aux blocs de la journée. Pour comparer ces
fees à la variation de supply, les états sont interrogés aux hauteurs
`premier_bloc - 1` et `dernier_bloc`. La période entre ces deux états couvre
**les mêmes blocs** ; ses timestamps diffèrent de quelques secondes des deux
minuits civils. Le CSV conserve donc distinctement :

- les bornes civiles de la journée et ses 86 400 secondes ;
- les hauteurs et timestamps des états de blocs utilisés ;
- `measured_block_interval_seconds`, la durée réelle entre ces états.

Chaque lot conserve la supply et l'inflation historiques à ses bornes, avec
les hauteurs, timestamps de bloc, timestamps d'observation et sources.
La hauteur demandée doit être confirmée dans l'en-tête de réponse
`x-cosmos-block-height` ou `grpc-metadata-x-cosmos-block-height`.
Une archive élaguée, une hauteur non confirmée ou une valeur invalide rend
le snapshot et les métriques dépendantes indisponibles, jamais nuls.

```
émission estimée lot = supply début × inflation annuelle début × durée réelle / (365 × 86400)
émission estimée journée = somme des estimations de tous ses lots
fees / émission estimée (%) = fees collectées de tous les blocs / émission estimée journée × 100
variation nette supply = supply après le dernier bloc − supply avant le premier bloc
fees / hausse nette supply (%) = fees collectées / variation nette supply × 100
```

L'émission reste une **estimation par lots à taux initial constant**, et non
une somme mesurée des émissions brutes. Les variations de taux entre lots
sont prises en compte ; celles à l'intérieur d'un lot ne sont pas intégrées.
Si une estimation de lot manque, l'estimation journalière et son ratio sont
indisponibles. Les ratios à dénominateur nul ou négatif restent indisponibles.
Les ratios journaliers ne sont pas la moyenne des ratios de lots.

Les champs `declared_*` viennent de `auth_info.fee.amount`. Les champs
`collected_*` utilisent les événements SDK `tx/fee` ou les événements du fee
market `fee_pay/fee` + `tip_pay/tip`. Les tips sont inclus une seule fois dans
les frais totaux et isolés dans les colonnes `collected_tip_atom` et
`collected_ibc_tip_atom`. Si les deux formats coexistent, leurs montants doivent
être cohérents, sans addition des deux représentations d'un même prélèvement.
Les transactions dont le message échoue ne sont pas exclues : elles peuvent
avoir payé des frais. Les autres dénominations ne sont pas ajoutées aux ATOM.

Si un événement de frais manque ou reste ambigu, les fees collectées de toute
la journée et les ratios associés restent indisponibles. Les fees déclarées
ne les remplacent pas. La couverture des blocs reste vérifiable indépendamment.

**Les frais collectés ne prouvent aucun burn.** `effective_burn_atom` et
`dilution_after_burn_atom` restent vides ; `burn_status=not_measured`.
La variation nette de supply n'isole ni émission brute ni burn. On ne soustrait
jamais les fees de cette variation ou de l'émission estimée.

## Prix ATOM

Chaque lot enregistre un spot CoinGecko avec le timestamp d'observation, le
timestamp de mise à jour du fournisseur et son URL. Un prix invalide, non
positif ou vieux de plus d'une heure reste indisponible.
`fee_valuation_usd_at_collection` désigne uniquement une valorisation du lot
au moment de sa collecte. Elle ne constitue pas un prix historique de ce lot.

Le CSV journalier ne réutilise **aucun spot actuel comme prix historique** :
`historical_daily_price_usd` et `daily_fee_usd` restent vides tant qu'une
source historique avec une méthode de valorisation explicite n'est pas ajoutée.
Les exécutions suivantes ne revalorisent jamais les lots précédents.

## Conservation des historiques

Les deux CSV racine `hub_revenue_daily.csv` et
`hub_revenue_with_inflation.csv` conservent chacun leurs **253 observations,
inchangées octet pour octet**, protégées par `tests/legacy_sha256.json`.
Les anciennes copies de `site/`, qui contenaient chacune une observation du
19 février 2026, sont conservées à l'identique dans `archives/legacy_site/`.
Les sources sont copiées, sans déplacement, vers `site/` à chaque publication.

Les couvertures de blocs et timestamps des snapshots anciens sont inconnus.
Leurs prix USD, taux d'inflation, ratios et mesures de dilution restent archivés
mais **non validés**. Aucune journée complète n'est reconstituée à partir de ces
échantillons. Les anciennes fenêtres de `hub_criterion1_windows.csv`, si ce
fichier existe, restent indépendantes et ne sont pas promues en journées
complètes. L'ancien script peut encore collecter des fenêtres explicitement,
mais le pipeline et le workflow utilisent désormais `criterion1_daily.py`.

Références techniques :
[prélèvement et événement de frais SDK](https://github.com/cosmos/cosmos-sdk/blob/main/x/auth/ante/fee.go),
[événements fee market](https://github.com/skip-mev/feemarket/blob/main/docs/SPEC.md),
[en-têtes des réponses historiques SDK](https://github.com/cosmos/cosmos-sdk/issues/24049).

## Mise à jour distante reçue pendant la correction

Le commit automatique `85e1f18` a ajouté une observation du 5 novembre 2026
et revalorisé les lignes historiques avec un nouveau spot. Ses deux CSV
complets (254 observations chacun) sont conservés sans modification dans
`archives/legacy_remote/85e1f18/`, avec leurs empreintes dans les tests.
Les 253 observations originales restent inchangées à la racine. La nouvelle
observation distante reste un échantillon de couverture inconnue ; elle
n'est pas ajoutée aux journées complètes ou aux ratios corrigés.

## Mesure de l'optimisation

Un essai réseau sur les mêmes blocs `33715279..33715298` (20 blocs, dont 8
non vides, et 12 transactions) a comparé les deux méthodes avec les mêmes
protections de débit : **33 requêtes et 23,50 s** pour la lecture individuelle,
contre **4 requêtes et 3,37 s** pour la lecture groupée. Les comptes de
transactions, la classification IBC, les frais déclarés, les frais collectés
et les tips étaient identiques. Les requêtes de snapshots macro et de prix,
communes aux deux méthodes, étaient exclues de cette comparaison. Ce gain
d'environ 7 fois sur cet essai ne garantit pas le même facteur sur chaque
journée ou chaque fournisseur. Aucun historique de production n'a été modifié
par l'essai. Chaque ligne d'avancement indique désormais la durée du lot traité.

Schéma du décodage local :
[Cosmos SDK TxRaw, TxBody, AuthInfo et Fee](https://github.com/cosmos/cosmos-sdk/blob/main/proto/cosmos/tx/v1beta1/tx.proto).

## Reprendre uniquement une publication GitHub Pages

La collecte et la publication du site sont deux étapes distinctes. Une erreur
`remote: Internal Server Error` pendant `git push origin gh-pages` indique
un refus interne de GitHub lors de la publication ; elle n'annule pas le
commit des checkpoints déjà réalisé sur `main`.

La publication est maintenant tentée au maximum trois fois, avec des attentes
de 15 puis 30 secondes. Chaque tentative relit l'état de `gh-pages` via
l'action de publication ; aucun push forcé ni changement de permissions n'est
ajouté. Si les trois tentatives échouent, le workflow reste en échec.

Pour republier sans refaire la collecte :

1. Envoyer ce correctif sur GitHub.
2. Dans **Actions → Criterion 1A, 1C, 1D — Hub economy → Run workflow**, choisir `main`.
3. Cocher **publish_only**, laisser `start_date` vide et lancer le workflow.

Les checkpoints existants sont validés puis le tableau de bord est régénéré
et publié. Aucun bloc n'est collecté dans ce mode. Il faut lancer une nouvelle
exécution pour utiliser ces options : relancer une ancienne exécution réutilise
sa définition de workflow antérieure. Les exécutions toutes les 30 minutes continuent de
collecter normalement et reprennent les journées incomplètes.

## Frais des transactions échouées et audit historique

Certaines transactions dont le message échoue conservent les événements du
prélèvement ante mais n'émettent pas le `fee_pay` du post-handler. Leur échec
ne prouve donc pas une absence de frais. Le collecteur peut maintenant vérifier
ce cas avec un ensemble strict de preuves :

- compte `feemarket-fee-collector` interrogé à la hauteur historique exacte,
  dont la hauteur de réponse est confirmée ;
- transaction échouée, sans événement de message exécuté (`action` ou `msg_index`)
  ni événement de frais contradictoire ;
- un seul transfert au compte du module, concordant avec son crédit et le débit
  du payeur, du payeur explicite ou du granter ;
- montant effectivement transféré identique aux coins déclarés, sans transfert
  de sortie du collecteur ou remboursement observé dans cette transaction.

Sans ces preuves concordantes, les frais restent inconnus. La réception par
le module ne constitue toujours aucune preuve de burn. Les tips indiquent
les distributions de tips attestées dans les événements de la transaction.

Pour les journées déjà collectées, `python criterion1_fee_audit.py 2026-11-04`
réexamine uniquement les lots concernés et enregistre les reçus et preuves dans
`data/criterion1_fee_audits/2026-11-04.json.gz`. La version finale est compressée sans perte, après vérification de son contenu.
La version de travail JSON permet la reprise après interruption. Il est lié à l'empreinte du checkpoint original et vérifie les
hauteurs, comptes de transactions et montants déclarés avant toute correction.
Les checkpoints originaux et leurs prix/snapshots restent inchangés. Un audit
entièrement terminé est appliqué en mémoire lors de l'export et de la génération
du dashboard ; un audit partiel ne débloque aucun ratio journalier.

## Déclenchement après une correction

Un push sur `main` touchant le workflow, les scripts, les dépendances ou les
tests déclenche désormais une nouvelle exécution. Les commits qui n'ajoutent
que des données et le HTML ne créent pas de boucle. La programmation toutes
les 30 minutes est décalée aux minutes 17 et 47, et reste soumise aux délais
de GitHub. Un événement `push` réussi ne prouve pas encore que le déclenchement
`schedule` fonctionne : cette vérification reste distincte dans les exécutions.
Les permissions administratives du dépôt ne sont pas lisibles sans connexion
GitHub ; les données publiques seules ne permettent pas d'exclure toutes les
restrictions de compte ou de service.

### Résultat de l'audit du 4 novembre 2026 UTC

Les 51 transactions initialement sans preuve de frais reconnue sont des
transactions échouées dont le transfert ante au module historique de frais
est attesté. L'audit a résolu les 51 cas, sans frais inconnus restants et sans
modifier le checkpoint original. Les 15 057 blocs et 11 663 transactions
restent identiques aux mesures initiales.

- Fees collectées, tips inclus : **75,928028 ATOM**, dont **8,548950 ATOM IBC**.
- Tips attestés : **48,928178 ATOM**, déjà inclus dans le total, jamais ajoutés deux fois.
- Fees / émission estimée sur les mêmes blocs : **0,0513269 %**.
- Fees / hausse nette de supply sur les mêmes blocs : **0,0407189 %**.
- Burn effectif : non mesuré ; aucun frais n'est soustrait de la dilution.
- Prix historique journalier et valorisation USD journalière : indisponibles.

Ces résultats sont exportés avec `fee_audit_status=verified` et
`fee_evidence_recovered=51`. Ils ne prouvent pas encore que les déclenchements
planifiés GitHub Actions ont repris : ce point doit être confirmé par un
événement `schedule` après publication du correctif.

## Sous-critère 1B — centralité des flux IBC

Le pipeline séparé compare Cosmos Hub, Osmosis, Noble et Injective.
Sa configuration, ses limites de couverture, les unités par actif, les prix
historiques et les instructions sont décrits dans [CRITERION2.md](CRITERION2.md).
Le pipeline se lance avec `python run_criteria1b_pipeline.py`. Sa page dédiée est `site/criterion2.html`.
# Dashboard web

L'accueil réunit désormais les sous-critères du critère 1 avec graphiques, filtres de période,
état des collectes et téléchargements. Les détails et la préparation du domaine
personnalisé sont documentés dans [DASHBOARD.md](DASHBOARD.md).
