# Critère 2 — activité relative des flux IBC

Ce critère compare **Cosmos Hub, Osmosis, Noble et Injective**.
La configuration est dans `config/criterion2_chains.json`. Un identifiant de
périmètre est calculé à partir des chaînes et du protocole ; ajouter une chaîne
crée un nouveau périmètre sans mélanger ses parts avec l'ancien dénominateur.

## Entrées ajoutées : registre séparé

Les sorties historiques conservent leur périmètre et leurs checkpoints.
Les entrées sont collectées séparément dans `data/criterion2_incoming/`, avec
un identifiant propre au périmètre `ics20_v1_received_ack_observed`.
Le filtre est `recv_packet.packet_dst_port='transfer'`. Le jour est celui du
bloc de réception sur la chaîne destinataire, pas le jour d'émission.

Chaque transaction réussie est contrôlée pour distinguer :

- **réception observée** : événement `recv_packet` ICS20 v1 ;
- **entrée confirmée** : `write_acknowledgement` correspondant aux deux ports,
  deux canaux et à la séquence, avec le succès standard ICS20 (`result: AQ==`),
  observé dans la même transaction ;
- **accusé d'erreur** : réception exclue des montants entrants confirmés ;
- **non confirmé** : accusé absent, asynchrone ou format de middleware non
  interprété, conservé séparément sans supposer un succès ou un échec.

Les accusés écrits dans une transaction ultérieure ne sont pas rapprochés dans
cette version. En particulier, certains accusés de contrats/IBC hooks sont
classés non confirmés même s'ils peuvent représenter un succès : la part des
entrées confirmées est donc une part de ce sous-ensemble, potentiellement biaisée.
Les transactions échouées n'ajoutent aucune réception. Les événements bancaires
génériques ne sont pas utilisés comme preuve d'un transfert IBC entrant.
Un contenu malformé ou un montant nul rejeté par l'application reste une
réception rejetée/non confirmée observée, avec montant indisponible ; il ne
devient pas une entrée de zéro token. Un succès standard accompagné d'un
contenu incompatible bloque la validation au lieu d'inventer un montant.

La dénomination locale est reconstruite suivant
[ICS20](https://github.com/cosmos/ibc/blob/main/spec/app/ics-020-fungible-token-transfer/README.md)
et le [traçage de source](https://github.com/cosmos/ibc-go/blob/main/docs/architecture/adr-001-coin-source-tracing.md) :
retrait du préfixe du canal source lors d'un retour, sinon ajout du canal
destinataire avant rapprochement avec le Chain Registry. Un port distant peut
être un contrat ; le port local est `transfer`. Aucun montant n'est converti
selon les unités du token natif par défaut.

Seules les entrées avec succès confirmé sont valorisées aux prix historiques
datés déjà utilisés par les sorties. Les quatre chaînes doivent avoir la même
journée indexée complète pour publier une comparaison. Le total USD reste vide
si des réceptions ne sont pas confirmées ou si une entrée confirmée n'a pas de
valorisation. Les échecs et inconnues ne sont jamais présentés comme un total nul.

Exports ajoutés à la racine et dans `site/` : `criterion2_incoming_progress.csv`,
`ibc_in_chains_daily.csv`, `ibc_in_assets_daily.csv`, `ecosystem_in_usd_daily.csv`.
Le dashboard français et anglais propose un choix de sens des flux ; les
tableaux détaillés décrivent également les réceptions rejetées ou non confirmées.
La rubrique de qualité suit séparément l'avancement entrant de chaque chaîne.

Le collecteur exécute les deux sens par défaut avec un budget total réparti
(un quart pour les sorties, trois quarts pour les entrées). `--direction in`
ou `--direction out` permet une reprise indépendante. Une erreur d'un sens
n'empêche pas la tentative de l'autre ; elle reste signalée en fin d'exécution.
Les plages vérifiées sont conservées pour l'exécution suivante.

Les entrées et sorties ne constituent pas un solde net : elles peuvent concerner
des jours, actifs et couvertures de valorisation différents. Un accusé de succès
ne prouve pas un crédit définitif au bénéficiaire à travers tous les sauts.
La mesure du transit et le rapprochement de bout en bout restent hors périmètre.

## Ce que les chiffres sortants mesurent

- Les paquets **ICS20 v1** émis avec `send_packet.packet_src_port='transfer'`.
- Les montants bruts émis, par actif, chaîne et journée UTC terminée.
- La part du Hub dans les émissions des quatre chaînes, en nombre de paquets
  et dans le sous-ensemble valorisé en USD de référence historique.

Il ne s'agit pas d'un volume net reçu : les acknowledgements, erreurs,
timeouts et remboursements ne sont pas réconciliés. Un même mouvement peut
produire plusieurs sauts IBC et plusieurs émissions. Les autres ports,
NFT, IBC v2, CCTP et transferts natifs ne sont pas inclus. Une part des sorties
sur ce périmètre ne prouve ni une centralité de routage universelle du Hub,
ni une part du marché des RWA. Ces indicateurs constituent la base de mesure,
pas un verdict automatique validant le critère économique.

## Première journée collectée : 5 novembre 2026 UTC

| Chaîne | Transactions de l'index contrôlées | Paquets émis retenus |
|---|---:|---:|
| Cosmos Hub | 927 | 928 |
| Osmosis | 4 415 | 4 418 |
| Noble | 1 047 | 1 050 |
| Injective | 605 | 606 |

Les quatre journées sont terminées selon la méthode décrite ci-dessous.
Sur Injective, le bloc 190442400 présentait une réponse d'index tronquée : ses
27 transactions et résultats d'exécution ont été contrôlés directement.
Les tentatives de pagination incohérentes restent archivées et exclues des exports.
Les valorisations restent explicitement partielles si un actif ou son prix manque.

## Collecte et exhaustivité selon l'index

Le pipeline identifie les blocs qui encadrent les deux minuits UTC sur chaque
chaîne. Il exige la bonne chaîne, un nœud synchronisé, un index explicitement
activé et un bloc finalisé à partir du minuit suivant. Les transactions sont
recherchées uniquement entre ces hauteurs incluses. Le timestamp du bloc à
minuit suivant est exclu de la journée précédente.

La source de couverture est **l'index d'événements du fournisseur**. Ce n'est
pas un audit indépendant de tous les blocs : un index incomplet mais cohérent
peut omettre des transactions sans que sa propre réponse le révèle. Cette
limite est indiquée dans les CSV et sur la page. Aucun pourcentage de blocs
physiquement scannés à 100 % n'est revendiqué.

La méthode recommandée découpe la journée en plages de hauteurs adaptatives.
Une plage n'est acceptée que si une réponse contient tous ses résultats
annoncés, avec au plus 100 transactions. Sinon la plage est divisée. Si même
une hauteur isolée donne une réponse incomplète, le collecteur vérifie le bloc
et **tous** ses résultats d'exécution, contrôle leur nombre et extrait ses
paquets directement. Ce recours est enregistré comme `block_results_verified`.
Les plages doivent couvrir les hauteurs sans trou ni chevauchement.

Le mode de pagination classique reste lisible pour les checkpoints déjà
collectés. Chaque page y est contrôlée : taille exacte, total stable, ordre,
unicité des hashes et des identifiants de paquets. Un contrôle final confirme
le total et le premier résultat. Une interruption après la dernière page mais
avant validation peut reprendre sans relire ou doubler les pages.

Dans les deux modes, chaque résultat est vérifié contre la hauteur, le filtre
IBC et le hash SHA-256 de ses octets. Plusieurs paquets d'une même transaction
sont conservés séparément. Une transaction échouée ne produit aucun flux émis
retenu. Un format de paquet inconnu n'est pas transformé en volume nul.

Un fournisseur est figé dans chaque plan : ses résultats ne sont pas mélangés
à ceux d'un autre index au milieu de la collecte. Si les réponses deviennent
incohérentes, l'essai est conservé dans `data/criterion2/rejected/` et une
nouvelle tentative utilise un autre fournisseur ou la nouvelle méthode.
Les essais rejetés n'entrent jamais dans les totaux exportés.

## Unités et prix

Les données d'actifs viennent du [Cosmos Chain Registry](https://github.com/cosmos/chain-registry),
avec source, horodatage et empreintes enregistrés dans le plan du jour.
Chaque actif utilise ses propres exposants : ATOM/OSMO/USDC utilisent notamment
6 décimales, **INJ utilise 18 décimales**. Les vouchers ICS20 sont rapprochés de
leur dénomination bancaire locale par le hash SHA-256 du chemin de trace.
Un actif non reconnu conserve son montant brut mais n'a aucune valeur USD.

Aucun total de montants bruts de dénominations différentes n'est calculé.
Les valeurs par actif restent séparées dans `ibc_out_assets_daily.csv`.
Les métadonnées observées ne sont pas présentées comme un historique certain
des propriétés d'un actif avant leur date d'observation.

Les prix utilisent l'[API historique CoinGecko](https://docs.coingecko.com/reference/coins-id-history) :
référence à 00:00 UTC de la date demandée, avec date, identifiant d'actif,
source, paramètres et timestamp de collecte. Ce prix de référence commun ne
représente pas le prix d'exécution de chaque transfert. Aucun spot actuel ne
remplace un prix historique manquant ; **USDC n'est pas fixé à 1 USD**.
Un prix valide est figé ; un échec est stocké séparément et pourra être réessayé.

L'accès public peut être limité par le fournisseur. Les secrets optionnels
`COINGECKO_DEMO_API_KEY` et `COINGECKO_PRO_API_KEY` sont transmis exclusivement
par en-tête au domaine officiel correspondant, jamais dans les fichiers ou
messages d'erreur.

## Comparaisons et données manquantes

Une ligne de `ecosystem_out_usd_daily.csv` nécessite la **même journée UTC
terminée sur toutes les chaînes du périmètre**, selon les index contrôlés.
Une chaîne inaccessible ou incomplète ne devient jamais une chaîne à zéro.

- `valued_out_usd` : sous-total des paquets dont actif, unités et prix sont connus.
- `unpriced_packets` : nombre de paquets non valorisés, pas une estimation de leur valeur.
- `hub_share_valued_usd_pct` : part du Hub **dans ce sous-ensemble valorisé**.
- `total_out_usd` et `hub_share_total_usd_pct` : disponibles uniquement si aucun
  paquet du périmètre n'est non valorisé.
- `hub_share_packet_count_pct` : part des paquets émis, sans mélanger les unités
  des tokens ; un paquet n'est pas un dollar ni un utilisateur distinct.

Le sous-ensemble valorisé peut être biaisé par les actifs absents du registre
ou sans prix. Sa part ne doit pas être présentée comme une part totale de marché.
Aucun ratio à dénominateur nul n'est remplacé par un faux zéro.

## Fichiers et exécution

```sh
python -m unittest discover -s tests -p 'test_criterion2*.py' -v
python run_criteria2_pipeline.py
```

La première exécution vise hier en UTC. Pour initialiser un autre début sur
un historique vide : `START_DATE=2026-11-05 python run_criteria2_pipeline.py`.
Les exécutions suivantes terminent d'abord la journée incomplète, puis les
jours suivants. Changer `START_DATE` ne peut pas remplacer l'historique.

Les checkpoints immuables sont dans
`data/criterion2/<perimetre>/<chaine>/<jour>/` : plan, pages ou plages compressées,
puis marqueur de complétion. Les snapshots de prix sont dans
`data/criterion2/prices/<jour>/`. Un budget de temps par chaîne empêche une
chaîne lente de consommer toute l'exécution. Les fichiers sont écrits par
remplacement atomique et les doublons sont refusés.

Exports racine, puis copies dans `site/` :

| Fichier | Contenu |
|---|---|
| `criterion2_progress.csv` | Avancement et fournisseur de chaque chaîne/jour |
| `ibc_out_assets_daily.csv` | Montants par actif et par chaîne, journées complètes uniquement |
| `ibc_out_chains_daily.csv` | Paquets, sous-totaux USD et couverture de valorisation |
| `ecosystem_out_usd_daily.csv` | Comparaisons sur les journées communes du périmètre |
| `site/criterion2.html` | Page dédiée avec limites méthodologiques visibles |

`.github/workflows/daily_criteria2.yml` est indépendant du pipeline du critère 1.
Il est programmé chaque heure à la minute 07 et se déclenche aussi sur les
modifications de ses scripts/configuration/tests. Les heures GitHub Actions
restent indicatives. Le budget par défaut est de 20 minutes, réparti entre
les chaînes ; les pages/plages terminées sont conservées après un échec.
L'export de prix dispose de son propre budget limité.

Les deux workflows utilisent le même verrou d'écriture du dépôt pour éviter
les pushes et publications simultanés. La page du critère 2 est publiée à part ;
le workflow du critère 2 ne régénère pas les calculs ou CSV du critère 1.
Les erreurs de collecte sont signalées après sauvegarde du progrès vérifié,
et les publications GitHub Pages disposent de trois tentatives espacées.
Le mode `publish_only` permet de régénérer l'export et la page sans collecter.

## Lifecycle reconciliation (v1)

`criterion2_lifecycle_collect.py` scans closed UTC days independently for
`write_acknowledgement`, `acknowledge_packet`, `timeout_packet` and the legacy
`timeout_on_close_packet` index, on all four configured chains. Each compressed
journal contains validated, contiguous height ranges, a fixed provider, packet
identities and hashed payload/evidence. Saved ranges are append-only; complete
journals cannot be rewritten. The hourly workflow spends up to 600 seconds on
this stage and resumes unfinished journals on the next run. The original
send/receive checkpoints and historical volume CSVs are preserved.

`criterion2_lifecycle_export.py` attaches evidence to **complete original packet
cohorts**, by local chain, both ports/channels, sequence and exact payload SHA256.
Evidence can occur in later transactions and later closed days; it never moves
the original cohort date. Incoming state uses local destination write-ack proof;
outgoing state uses local source execution proof. These are separate perspectives,
not a cross-chain topology join or a multi-hop settlement guarantee.

Source core events alone are insufficient: IBC may emit them on NOOP replays.
Raw protobuf messages and TxMsgData responses (including authz wrappers) must
prove result `SUCCESS=2`; NOOP, absent and unknown responses cannot confirm.
Standard ICS20 acknowledgements and explicit Osmosis IBC-hooks / ICS29 fee
middleware envelopes are decoded with bounded schemas. Unsupported envelopes
stay unknown; contradictory terminal outcomes or payloads stay conflicts.
An elapsed timeout timestamp is never treated as a confirmed timeout.

A refund is verified only for an executed source error/timeout with matching
application context and one explicitly message-indexed bank transfer, recipient
credit and sender debit. Recipient, amount and local bank denomination must match
the original packet. Ambiguous multi-message credits, ante-fee movements and
missing proof do not count. Incoming error refunds on remote sources remain
unverified. A failure and a verified refund are distinct fields.

New exports (also published under `site/`):
- `ibc_lifecycle_daily.csv`: per-chain/direction/original-day counts, plus search coverage.
- `ibc_lifecycle_progress.csv`: provider and per-stream scanned/expected heights.
- `ibc_packet_lifecycle.csv.gz`: per-packet outcomes, evidence IDs and refund proof hashes.

All outcome counts partition the original cohort. Refund counts are a subset of
confirmed failures, not an additional outcome. `coverage_complete` means all
required local indexes cover the cohort day; `complete_through_utc` is the minimum
contiguous exclusive UTC boundary across those streams, starting at that cohort.
A missing subsequent day stops coverage even if a later journal exists. Positive
proof from a partial journal may be shown, but search coverage stays incomplete.
`latest_evidence_day` records the latest journal day of matched positive proof,
which can extend beyond complete search coverage when a later journal is partial.
Completed search is **not** equivalent to all packets having settled. Event-index
completeness remains conditional on the provider, as in the original ledger.
No net flows, USD settlement total or transit metric is calculated in this step.

Protocol references: ibc-go v8.8.0 `proto/ibc/core/channel/v1/tx.proto`,
`modules/core/keeper/msg_server.go`, `modules/apps/transfer/ibc_module.go`;
Osmosis `x/ibc-hooks/wasm_hook.go`. Unsupported legacy message-response formats
remain unverified rather than being inferred from an event.

## Étape 2 — flux nets des transferts réussis

`criterion2_net.py` calcule **entrées réussies − sorties réussies**, par chaîne,
jour UTC et dénomination bancaire locale exacte. Le jour est celui de réception
pour les entrées et d'émission pour les sorties : ce sont des **cohortes
journalières d'activité**, pas des mouvements bancaires datés au règlement.
Un accusé ultérieur peut préciser une cohorte antérieure. Une erreur ou un
timeout confirmé exclut le transfert réussi ; un remboursement n'est jamais
ajouté une seconde fois. Ce calcul ne prouve pas une variation de solde bancaire,
une accumulation de capital ou le règlement au bénéficiaire final multi-sauts.

Les huit cohortes (deux sens × quatre chaînes) doivent couvrir complètement la
même journée UTC selon les index. Les hauteurs limites des plans entrant et
sortant doivent être identiques sur chaque chaîne. Une collecte manquante ou
partielle produit un état indisponible, jamais un zéro ni une extrapolation.
La borne des preuves est le **minimum des couvertures journalières contiguës de
tous les index de cycle de vie nécessaires, sur toutes les chaînes et les deux
sens**. Seuls les journaux complets antérieurs à cette borne exclusive sont
utilisés. Un succès connu plus tard dans le suivi de cycle de vie peut donc
rester inconnu dans le net jusqu'à ce que la couverture commune le rejoigne.

Les traces ICS20 sont ramenées à leur dénomination bancaire locale avant
soustraction. Les symboles identiques ne suffisent jamais à regrouper des tokens,
et deux vouchers de chemins différents restent distincts. Les montants bruts
sont calculés en entiers Python ; les unités et prix utilisent Decimal, y compris
pour les 18 décimales INJ et les différences d'une unité minimale entre montants
très grands. Les métadonnées contradictoires entre plans bloquent la conversion,
sans utiliser les décimales du token natif par défaut.

Un paquet incertain bloque le solde complet de **son actif** et le total USD de
la chaîne. Si un contenu incertain ne permet pas d'identifier l'actif, tous les
soldes complets de la chaîne restent indisponibles. Le sous-total connu conserve
son nom explicite dans le fichier par actif. Une absence de mouvement dans un
sens n'est un zéro que si les deux cohortes et la couverture commune sont
complètes. Les erreurs observées ne deviennent pas des entrées de zéro token.

Les deux nouveaux exports racine, copiés dans `site/`, sont :
- `ibc_net_assets_daily.csv` : brut exact, montants convertis, succès, échecs exclus et incertains,
  solde connu et solde complet, métadonnées, prix historique et borne des preuves.
- `ibc_net_chains_daily.csv` : couvertures, nombres de paquets/actifs, sous-totaux
  USD appariés et totaux USD seulement si tous les actifs et paquets sont couverts.

Pour le sous-total USD, un actif doit être résolu et valorisé, puis figurer dans
**le même ensemble dans les deux termes** de la soustraction de chaque chaîne.
Un actif dont la sortie est incertaine n'apporte pas seulement son entrée positive
au sous-total. La couverture des actifs peut différer entre chaînes : les
sous-totaux ne constituent pas un classement ni une part de marché. Aucun
montant brut de tokens différents n'est additionné, et aucun total USD manquant
n'est remplacé par zéro. Les références de prix sont celles du jour de la cohorte
pour les deux sens, pas du jour d'un accusé ultérieur ; jamais de spot rétroappliqué.
La collecte des prix peut inclure un actif reçu avant confirmation asynchrone ;
posséder un prix ne confirme pas son transfert.

Les exports conservent les lignes d'autres périmètres historiques ; le dashboard
filtre le périmètre courant. Une disparition de cohorte/actif déjà exporté ou une
régression de couverture bloque la réécriture. Les anciens CSV de flux bruts et
les preuves immuables sont préservés. `publish_criterion2.py` régénère ces nouveaux
exports à chaque exécution, sans ajouter de requête réseau au calcul des nets.
La configuration horaire du workflow reste inchangée ; les deux nouveaux CSV
racine et publiés sont ajoutés au commit automatique.

Première cohorte commune, 5 novembre 2026 UTC, preuves avant le 6 novembre à
00:00 UTC : ATOM natif sur le Hub, 116 879,948760 entrants réussis moins
110 630,697557 sortants réussis = **+6 249,251203 ATOM**. Ce résultat par actif ne
rend pas disponible le total USD tous actifs, dont la couverture reste partielle.

## Cumuls calendaires sur 7, 30 et 90 jours

`criterion2_windows.py` lit les deux CSV quotidiens de flux nets et calcule les
fenêtres inclusives de 7, 30 et 90 jours, terminées à la dernière journée UTC
close lors du calcul. La borne ne recule pas jusqu'à la dernière observation
pour masquer un retard. Chaque bilan indique ses dates, jours couverts,
jours manquants et la liste explicite de ces journées.

Un jour couvert exige les cohortes communes complètes des quatre chaînes dans
les deux sens, selon les index des fournisseurs (la même réserve de couverture
que les séries quotidiennes). Un jour manquant ou incomplet est exclu du cumul
observé et bloque le bilan complet. Il ne devient jamais un zéro. Une absence
d'un actif n'est un zéro que sur une journée entièrement couverte, avec un registre
d'actifs complet et sans contenu incertain pouvant appartenir à cet actif.

Les montants bruts sont additionnés en entiers par dénomination locale, puis
convertis avec des unités cohérentes. Des unités ou identifiants de prix
contradictoires entre journées bloquent la conversion. Un état incertain sur
une journée active bloque le net observé de cet actif, ainsi que le total complet.
Des bornes de preuves différentes entre jours bloquent les montants cumulés ;
on n'additionne pas des états de confirmation arrêtés à des dates différentes.

Le sous-total USD utilise un ensemble d'actifs constant dans les deux sens et
sur toute la fenêtre observée : un actif doit être résolu et valorisé sur chacune
de ses journées actives. Un prix manquant une seule journée exclut cet actif
entier du sous-total USD de la période. On additionne ses valorisations journalières
historiques ; on ne revalorise pas tous les jours au dernier prix. Les couvertures
peuvent différer entre chaînes : les sous-totaux ne sont pas un classement.

Deux nouveaux fichiers sont publiés à la racine et dans `site/` :
- `ibc_net_windows_chains_latest.csv` : derniers bilans des fenêtres, couverture,
  cumuls USD observés et totaux complets seulement quand tous les jours et actifs
  sont couverts.
- `ibc_net_windows_assets_latest.csv` : cumuls exacts par actif, unités, états,
  net observé sur les jours couverts et net complet de la fenêtre séparés.

Ce sont des vues calculées du dernier bilan, pas de nouveaux relevés historiques.
Les CSV quotidiens, checkpoints et preuves existants ne sont pas modifiés.
Le workflow du critère 2 recalcule et commit ces vues avec les données quotidiennes,
sans nouvelles requêtes réseau pour l'agrégation. La publication du critère 1
réutilise le dernier bilan disponible et ne dépend pas de son recalcul. Si la
fenêtre n'atteint plus hier en UTC, le dashboard signale le retard d'actualisation.

Le sélecteur **Bilan : Jour / Cumul 7 jours / Cumul 30 jours / Cumul 90 jours**
pilote les cartes et le tableau des nets. La vue par défaut est 7 jours. Le
filtre **Historique affiché** continue de régler le graphique quotidien, dont
les trous ne sont jamais reliés. En historique court, les cartes sont explicitement
libellées « observées » et « sur les jours couverts » ; le bilan complet reste
indisponible. Aucun remplissage ni récupération automatique de journées antérieures
n'est déclenché par ce changement.

## Étape 3 — transit PFM prouvé sur le Hub

`criterion2_transit_collect.py` examine uniquement les transactions du Cosmos Hub
présentes à la fois dans les cohortes complètes de réceptions et d'émissions de la
même journée UTC. Cette liste est un filtre de candidats, jamais un compteur de
transit. Pour attribuer une première émission, `criterion2_transit_evidence.py`
exige une instruction PFM explicite, un `MsgRecvPacket` exécuté (réponse enum 2,
NOOP exclu), l'identité et le hash du paquet entrant, le compte intermédiaire PFM
dérivé du canal et de l'expéditeur, le destinataire/canal/mémo `next` attendus,
le même actif et montant, et des crédits/débits bancaires uniques dans le même
`msg_index`. Un montant ou un horaire commun ne suffit pas. Les liens sont
bijectifs : aucune réception ni émission n'est comptée deux fois.

Les requêtes historiques `Channel` et `ChannelClientState` doivent répondre à la
hauteur exacte de la transaction. Canal OPEN, port, canal de contrepartie et
connexion doivent correspondre aux événements. Seuls les clients Tendermint et
les versions ICS20 reconnues sont interprétés. Deux canaux différents vers la
même chaîne sont un **retour à la source**, pas un transit vers une autre chaîne.
Une topologie absente reste inconnue ; la topologie actuelle ne remplace jamais
une réponse historique indisponible. Les données RPC sont des preuves fournies
par le prestataire, pas un audit cryptographique indépendant par client léger.

`criterion2_transit_export.py` utilise une borne UTC commune aux index locaux
entrants et sortants du Hub ; les journaux incomplets et postérieurs sont exclus.
Un **transit confirmé** exige le succès du paquet entrant ET de la première
émission liée, avec chaînes source et prochain saut distinctes. Un succès parent
après timeout de la première émission peut correspondre à une nouvelle tentative :
il reste indéterminé sans preuve du lien de cette tentative. Les échecs confirmés
restent séparés et sont exclus des montants. Le prochain saut ne prouve ni le
bénéficiaire final ni le règlement de tous les sauts ultérieurs.

Cette première version mesure un **sous-ensemble automatique PFM**. Les transferts
manuels, différés, autres mécanismes et nouvelles tentatives sans lien prouvé ne
sont pas attribués. Une réception sans lien n'est pas déclarée non transitaire.
La part affichée est le minimum identifié parmi les **réceptions observées du
Hub, en nombre de paquets**, pas une part de marché ni une centralité globale.
La complétude des candidats et des métadonnées historiques est affichée ; une
collecte partielle ne devient jamais un total de journée extrapolé.

Le bundle `data/criterion2_transit/<univers>/cosmoshub/<date>/observations.json.gz`
conserve les transactions et réponses historiques brutes. Chaque sauvegarde
vérifie que toutes les observations antérieures sont conservées à l'identique.
Les requêtes groupées valident tous les identifiants de réponse ; un fournisseur
sans batch utilise les requêtes individuelles. Le budget de 300 secondes par
workflow conserve les checkpoints pour l'exécution suivante et signale les
erreurs après sauvegarde. `publish_only` ne lance aucune collecte de transit.

Quatre nouveaux CSV sont publiés à la racine et dans `site/` :
- `ibc_transit_daily.csv` : couverture, liens, états et part minimale en paquets ;
- `ibc_transit_routes_daily.csv` : chaîne source → Hub → prochain saut ;
- `ibc_transit_assets_daily.csv` : montants des seuls transits confirmés par actif ;
- `ibc_transit_packets.csv` : identifiants parents/enfants, états et hash des preuves.

Les unités et prix historiques proviennent des snapshots existants de la journée
de réception. Aucun spot n'est rétroappliqué, aucune parité USDC n'est imposée,
et des tokens différents ne sont jamais additionnés en unités brutes. Les exports
C1, sorties/entrées, confirmations, nets et fenêtres gardent leur définition et
leurs historiques. Le dashboard FR/EN lit uniquement ces exports ; la publication
C1 ne déclenche aucune nouvelle collecte ni aucun recalcul du transit.

Références officielles du modèle : Gaia v29.0.1 épingle PFM v10.6.0 et SDK v0.53.8
([go.mod](https://github.com/cosmos/gaia/blob/v29.0.1/go.mod)). Dérivation du compte,
réception, émission et ACK asynchrone :
[keeper.go](https://github.com/cosmos/ibc-apps/blob/middleware/packet-forward-middleware/v10.6.0/middleware/packet-forward-middleware/packetforward/keeper/keeper.go),
[ibc_middleware.go](https://github.com/cosmos/ibc-apps/blob/middleware/packet-forward-middleware/v10.6.0/middleware/packet-forward-middleware/packetforward/ibc_middleware.go),
[hash.go SDK](https://github.com/cosmos/cosmos-sdk/blob/v0.53.8/types/address/hash.go).
Le modèle reconnaît ces preuves et s'abstient lorsqu'elles ne concordent pas ;
il ne certifie pas à lui seul la version applicative historique du nœud.

## Identification des produits RWA

Un premier registre daté, non exhaustif et séparant produits tokenisés et
stablecoins est documenté dans [RWA_REGISTRY.md](RWA_REGISTRY.md).
L'audit hors ligne identifie USDY par origine exacte et preuves de canaux
historiques. USYC reste candidat tant que son identité mainnet n'est pas établie.
Le filtre RWA du dashboard affiche les montants confirmés et le solde par
produit et par journée, ainsi que les transits PFM identifiés. Les identités
en attente sont signalées et les montants restent en unités du produit.
