# Dashboard web

Le sélecteur Français / English relie `index.html` à `index-en.html`.
Les deux versions embarquent exactement les mêmes mesures. Les pages détaillées
existent aussi en anglais (`criterion1-en.html`, `criterion2-en.html`).
Les traductions sont dans `web/en.json` ; les formats de nombres, de dates et
les messages des graphiques suivent la langue de la page. Les CSV sources
conservent leurs colonnes et valeurs originales.

Le critère 2 propose désormais un choix **Sorties émises / Entrées avec succès
confirmé**, avec des dates et historiques indépendants. Les réceptions observées,
accusés d'erreur et confirmations manquantes sont distingués dans l'avis de
qualité et dans les tableaux détaillés. Les quatre nouveaux CSV entrants sont
téléchargeables ; aucune addition entrées + sorties ou soustraction de solde
net n'est effectuée. Les limites exactes des accusés sont dans `CRITERION2.md`.

La page d'accueil `site/index.html` regroupe les deux critères, leurs graphiques
et l'état des collectes. `site/criterion1.html` et `site/criterion2.html`
conservent les tableaux et méthodes détaillés. Le nom de présentation est
« Cosmos Observatory » ; aucun nom de domaine n'est réservé ou acheté.

## Actualisation

Les sources de présentation sont dans `web/`. `build_web_dashboard.py` lit
les exports enregistrés et les checkpoints du critère 1 sans les modifier.
Il intègre les données à la page et copie les deux fichiers CSS/JS. Le site
n'utilise aucun CDN, service d'analytics, compte utilisateur ou API externe
dans le navigateur. Il peut donc aussi être consulté localement.

Les générateurs des deux critères reconstruisent l'accueil après chaque
exécution ; chaque workflow sauvegarde l'accueil et ses assets avec ses données.
Le verrou commun de publication reste en place. Les collectes et leurs
fréquences restent celles du projet existant.

```sh
python generate_dashboard_html.py
python publish_criterion2.py
python -m unittest discover -s tests -v
node --test tests/dashboard.test.js
python -m http.server 8765 --directory site
```

L'aperçu local est alors disponible à `http://localhost:8765`.

## Lecture

- Les cartes montrent la dernière journée complète dans la période sélectionnée,
  avec une date propre à chaque critère, pas une somme sur la période.
- Le filtre de période se termine à la dernière date publiée dans l'un des deux
  critères. Les graphiques représentent les observations disponibles sans
  relier les journées manquantes. Une observation isolée n'est pas une tendance.
- Le critère 2 utilise seulement le périmètre configuré actuel. Les anciens
  périmètres restent dans les CSV, sans être mélangés aux parts du périmètre actuel.
- Les sous-totaux USD peuvent être partiels. La couverture de valorisation en
  pourcentage concerne le nombre de paquets, jamais leur valeur économique.
- Les montants exacts restent dans les CSV. L'arrondi et les nombres JavaScript
  servent exclusivement à l'affichage et aux graphiques.
- L'état des collectes porte sur tout l'historique, indépendamment du filtre.
  Un dernier jour publié antérieur à hier UTC est signalé en retard. L'heure
  de génération de la page ne constitue pas la date de fraîcheur des mesures.

## Nom de domaine indépendant

Le dashboard est compatible avec GitHub Pages sous un chemin de projet ou à
la racine d'un domaine personnalisé : les liens et assets sont relatifs.
Aucun domaine n'est configuré tant que son nom n'est pas choisi et possédé.

Après achat du domaine, suivre la documentation officielle GitHub Pages pour
sa vérification et les enregistrements DNS. Ajouter le domaine choisi dans
`site/CNAME` et dans les paramètres Pages du dépôt, puis activer HTTPS quand
le certificat est disponible. La publication envoie le dossier `site/`, donc
ce fichier doit être conservé dans le dépôt pour les prochaines exécutions.
Ne pas enregistrer les données d'un domaine hypothétique dans le projet.
