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
