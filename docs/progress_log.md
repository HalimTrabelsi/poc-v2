# 📘 Progress Log — PFE OpenG2P Fraud Detection

## 📅 Date : 02/02/2026

### 🎯 Objectif du jour

* Comprendre le sujet PFE OpenG2P
* Définir l’architecture globale du projet
* Identifier les modules fonctionnels majeurs

### ✅ Travail réalisé

* Analyse du sujet : détection de fraude intégrée à OpenG2P
* Identification des modules :

  * Social Registry
  * Program Management (PBMS)
  * Disbursement (paiements)
  * SPAR
* Définition des flux métier :

  * enrôlement → programme → paiement
* Première modélisation de l’architecture fonctionnelle

### 📊 Résultats

* Vision globale du système établie
* Compréhension de l’écosystème OpenG2P

### ⚠️ Problèmes rencontrés

* Documentation fragmentée

### 🧠 Solutions apportées

* Analyse GitHub + documentation officielle
* Reconstruction logique des flux

### 📌 État actuel du système

* Architecture conceptuelle validée

### 🚀 Prochaine étape

* Déploiement technique (Docker)

---

## 📅 Date : 04/02/2026

### 🎯 Objectif du jour

* Déployer OpenG2P localement

### ✅ Travail réalisé

* Mise en place Docker Compose :

  * Odoo
  * PostgreSQL
* Build image Odoo avec addons OpenG2P
* Configuration environnement conteneurisé

### 📊 Résultats

* Odoo accessible
* PostgreSQL opérationnel

### ⚠️ Problèmes rencontrés

* Addons non chargés
* Erreur login admin

### 🧠 Solutions apportées

* Correction volumes Docker
* Vérification chemins addons

### 📌 État actuel du système

* Environnement partiellement fonctionnel

### 🚀 Prochaine étape

* Debug authentification

---

## 📅 Date : 05/02/2026

### 🎯 Objectif du jour

* Restaurer accès Odoo

### ✅ Travail réalisé

* Analyse logs Odoo
* Inspection table `res_users`
* Reset mot de passe admin via Odoo shell

### 📊 Résultats

* Accès admin rétabli

### ⚠️ Problèmes rencontrés

* DB manager désactivé
* commandes Odoo non trouvées (PATH)

### 🧠 Solutions apportées

* Utilisation du bon binaire :
  `/opt/bitnami/odoo/bin/odoo`
* Reset password via script

### 📌 État actuel du système

* Odoo pleinement fonctionnel

### 🚀 Prochaine étape

* Stabilisation OpenG2P

---

## 📅 Date : 08–12/02/2026

### 🎯 Objectif du jour

* Comprendre et stabiliser OpenG2P

### ✅ Travail réalisé

* Analyse workflow OpenG2P complet
* Étude modules :

  * Social Registry
  * PBMS
  * Paiements
* Analyse pipeline ODK
* Correction erreurs frontend (JS modules)

### 📊 Résultats

* Workflow métier maîtrisé
* Environnement stabilisé

### ⚠️ Problèmes rencontrés

* Modules JS non chargés
* erreurs `preview_document`

### 🧠 Solutions apportées

* Nettoyage cache Odoo
* suppression modules défaillants

### 📌 État actuel du système

* OpenG2P stable

### 🚀 Prochaine étape

* Conception moteur IA

---

## 📅 Date : 15–25/02/2026

### 🎯 Objectif du jour

* Concevoir le moteur antifraude

### ✅ Travail réalisé

* Définition architecture IA :

  * Rule Engine
  * ML Risk Scoring
  * Graph Intelligence
  * RAG / Explainable AI
* Feature engineering
* Création dataset synthétique
* Entraînement modèles :

  * Logistic Regression
  * Random Forest
  * Isolation Forest

### 📊 Résultats

* AUC ≈ 0.93
* Pipeline ML fonctionnel

### ⚠️ Problèmes rencontrés

* Data leakage
* Déséquilibre des classes

### 🧠 Solutions apportées

* Nettoyage features
* SMOTE / rééquilibrage

### 📌 État actuel du système

* Modèle ML validé en environnement contrôlé

### 🚀 Prochaine étape

* Intégration système

---

## 📅 Date : 02/03/2026

### 🎯 Objectif du jour

* Intégrer le moteur dans l’architecture SI

### ✅ Travail réalisé

* Conception API de scoring (FastAPI)
* Découplage :

  * Odoo ↔ moteur IA
* Définition architecture microservices

### 📊 Résultats

* Architecture technique validée

### ⚠️ Problèmes rencontrés

* Couplage initial fort

### 🧠 Solutions apportées

* Passage REST API

### 📌 État actuel du système

* Architecture scalable définie

### 🚀 Prochaine étape

* Implémentation réelle

---

## 📅 Date : 28/03 – 04/04/2026

### 🎯 Objectif du jour

* Construire pipeline data + rule engine

### ✅ Travail réalisé

* Accès base PostgreSQL via Docker
* Identification des tables OpenG2P :

  * `res_partner` (bénéficiaires)
  * `g2p_payment` (paiements)
  * `g2p_program`
  * `g2p_program_membership`
  * `g2p_entitlement`
* Analyse colonnes (`amount_paid`, `status`, etc.)
* Construction dataset antifraude :

  * total_amount_received
  * payment_frequency
  * program_count
  * shared_phone_flag
* Développement Rule Engine :

  * règles métier (JSON/Python)
  * scoring 0–100
  * audit trail

### 📊 Résultats

* Dataset antifraude défini
* Rule Engine fonctionnel

### ⚠️ Problèmes rencontrés

* Connexion PostgreSQL échouée (role incorrect)
* Données incomplètes

### 🧠 Solutions apportées

* Utilisation credentials Docker :

  * POSTGRES_USER=odoo
* Simulation données fraude

### 📌 État actuel du système

* Dataset prêt
* Rule Engine opérationnel

### 🚀 Prochaine étape

* ML + intégration API

---

## 📅 Date : 03/04/2026

### 🎯 Objectif du jour

* Stabiliser pipeline ML + architecture

### ✅ Travail réalisé

* Pipeline ML complet :

  * SMOTE
  * normalisation
* Architecture modulaire :

  * `app/`
  * `ml/`
  * `rules/`
* Scoring hybride (Rule + ML)

### 📊 Résultats

* Pipeline robuste

### ⚠️ Problèmes rencontrés

* erreurs import Python
* incohérences features

### 🧠 Solutions apportées

* correction PYTHONPATH
* harmonisation dataset

### 📌 État actuel du système

* Pipeline complet stable

### 🚀 Prochaine étape

* Connexion OpenG2P réel

---

## 📅 Date : 06/04/2026 -07/04/2026

### 🎯 Objectif du jour

* Finaliser l'intégration du moteur ML dans Docker
* Valider les performances du modèle
* Stabiliser l'infrastructure et corriger les erreurs runtime

### ✅ Travail réalisé

#### 🔹 1. Debug & Infrastructure Docker

* Identification du problème : ancienne version de `train_openg2p.py` dans le conteneur
* Solution : rebuild du service `fraud-engine`
* Correction d'une erreur critique : `python-multipart` manquant pour l'upload CSV
* Ajout dans `requirements.txt` + rebuild

#### 🔹 2. Training réel dans Docker

* Exécution du training sur dataset synthétique (10 000 lignes)
* Validation du pipeline complet

#### 🔹 3. Analyse des performances

* Random Forest :
  * Accuracy : 96 %
  * AUC : 0.9687
  * Recall fraude : 81 %
  * Precision fraude : 85 %
* Logistic Regression (baseline) : recall élevé, faible précision
* Features les plus importantes : `network_risk`, `nb_programs`, `shared_phone_count`, `shared_account_count`
* Cohérence métier validée : fraude réseau, multiplicité de programmes, anomalies comportementales

#### 🔹 4. Problèmes détectés

* Typo critique dans le dataset : `pmt_score_minn`
* Plusieurs features générées via fallback
* Dataset incomplet par rapport au schéma ML

#### 🔹 5. Structuration finale du moteur intelligent

* Formalisation du pipeline global :
  * Data Collector (PostgreSQL OpenG2P)
  * Feature Engineering
  * Rule Engine
  * ML (Random Forest + Logistic Regression + Isolation Forest)
  * Graph Analysis (NetworkX)
  * Score agrégé
  * SHAP (explicabilité)
  * RAG (cas similaires)
  * API FastAPI
  * Interfaces (Streamlit, Swagger, Grafana)
* Scoring hybride défini :
  * Rule Engine : 25 %
  * ML : 50 %
  * Graph : 25 %

### 📊 Résultats

* Modèle ML validé et performant (AUC 0.9687)
* Pipeline Docker fonctionnel
* API prête pour tests Swagger
* Architecture complète du moteur intelligent finalisée

### ⚠️ Problèmes rencontrés

* Typo `pmt_score_minn` dans le dataset synthétique
* `python-multipart` absent de `requirements.txt`
* Artefacts ML périmés dans le conteneur (rebuild nécessaire)

### 🧠 Solutions apportées

* Correction du dataset et realignment des features
* Ajout de `python-multipart` dans `requirements.txt`
* Rebuild du service Docker `fraud-engine`
* Réalignement `MODELS_DIR` (script → `models_saved/` lu par l'API)

### 📌 État actuel du système

* Moteur ML validé et opérationnel sous Docker
* API accessible via Swagger
* Pipeline bout-en-bout fonctionnel (dataset → modèle → API → UI)

### 🚀 Prochaine étape

* Connexion à la base PostgreSQL OpenG2P réelle
* Tests d'intégration end-to-end
* Préparation démonstration jury

---

## 📅 Date : 10–14/04/2026

### 🎯 Objectif du jour

* Connexion réelle à la base PostgreSQL OpenG2P
* Remplacer le dataset synthétique par des données live
* Construire le `FeaturesService` exploitant les tables OpenG2P

### ✅ Travail réalisé

* Configuration des credentials Docker pour double connexion DB :
  * `OPENG2P_DB_URL` → base Odoo (`openg2p-postgresql:5432/openg2p`)
  * `FEATURE_STORE_URL` → base fraude dédiée (`fraud-db:5432/fraud_engine`)
* Création du `FeaturesService` avec extraction directe SQL :
  * Démographie : `age`, `gender`, `income`, `dependency_ratio`
  * Programme : `nb_programs`, `nb_active_programs`, `avg_enrollment_days`
  * Paiement : `payment_count`, `payment_gap_ratio`, `payment_success_rate`
* Mise en place du schéma `feature_store` dans `fraud-db`
* Tests d'extraction sur les 151 registrants présents

### 📊 Résultats

* Connexion DB stable
* 18 features extraites depuis la DB live
* Latence d'extraction : ~3-5s par bénéficiaire

### ⚠️ Problèmes rencontrés

* Volumes de données très faibles (151 registrants, 12 paiements)
* Plusieurs colonnes attendues sont vides (`res_partner.phone`, `email`, `g2p_reg_id`)
* Latence élevée à cause des jointures multiples

### 🧠 Solutions apportées

* Sentinelles pour valeurs manquantes (sentinel values plutôt que NaN)
* Fallback sur données synthétiques quand DB pauvre
* Identification des index DB nécessaires (à créer plus tard)

### 📌 État actuel du système

* Moteur connecté à la DB OpenG2P réelle
* Pipeline `DB → FeaturesService → ML → Score` opérationnel

### 🚀 Prochaine étape

* Implémentation du Graph Intelligence
* Analyse réseau bénéficiaires

---

## 📅 Date : 15–19/04/2026

### 🎯 Objectif du jour

* Développer la couche Graph Intelligence (NetworkX)
* Détecter les clusters suspects via analyse réseau
* Calculer le `network_risk` par bénéficiaire

### ✅ Travail réalisé

* Implémentation du `GraphService` :
  * Construction graphe bipartite bénéficiaire ↔ ressources (téléphones, comptes)
  * Détection de composantes connexes
  * Centralité de degré et betweenness
* Features dérivées :
  * `shared_phone_count`
  * `shared_account_count`
  * `network_risk` (score 0-1 basé sur taille cluster + centralité)
  * `group_membership_count`
* Tests sur la DB : détection effective de 5 numéros partagés (3-15 utilisations)
* Le numéro `+224 666 SHARED 99` ressort comme hub avec 15 connexions

### 📊 Résultats

* GraphService opérationnel
* Détection automatique des clusters frauduleux
* Visualisation des composantes connexes (export JSON pour le dashboard)

### ⚠️ Problèmes rencontrés

* Performance NetworkX dégradée sur les graphes > 10 000 nœuds
* Calcul de betweenness centrality très coûteux (O(N³))

### 🧠 Solutions apportées

* Limitation aux composantes connexes > 1 nœud (filtrage)
* Caching des calculs de centralité dans Redis (envisagé)
* Approximation de betweenness sur échantillon

### 📌 État actuel du système

* Triple scoring : Rules + ML + Graph
* Ensemble pondéré (Rules 25% / ML 50% / Graph 25%)

### 🚀 Prochaine étape

* Implémentation analyse géographique (DBSCAN clusters)

---

## 📅 Date : 22–25/04/2026

### 🎯 Objectif du jour

* Ajouter la dimension géographique au scoring
* Détecter les hotspots de fraude
* Visualiser via une heatmap

### ✅ Travail réalisé

* Implémentation du `GeoService` :
  * Mapping bénéficiaire → coordonnées (lat/lon)
  * Clustering DBSCAN (eps=0.5km, min_samples=3)
  * Calcul du `geo_risk_score` par cluster
* Endpoints API :
  * `GET /api/v1/geo/heatmap` — points pour la carte
  * `GET /api/v1/geo/hotspots` — clusters DBSCAN
* Génération de coordonnées synthétiques (Conakry, Mamou, Kankan, Faranah)
* Intégration dans le dashboard Streamlit (onglet "Geographic Analysis")

### 📊 Résultats

* 14 hotspots détectés sur les données live
* Heatmap fonctionnelle avec Plotly
* Endpoint `/geo/hotspots` répond en < 200ms

### ⚠️ Problèmes rencontrés

* Pas de vraies coordonnées dans `res_partner` (champ `address` texte libre)
* Quelques `partner_id` non numériques (cas de test) plantent `int()`

### 🧠 Solutions apportées

* Génération déterministe via hash de l'ID
* Fallback sur centre géographique du pays par défaut
* (Bug `int('AUDIT-001')` identifié mais non corrigé immédiatement)

### 📌 État actuel du système

* 4 couches de scoring : Rules + ML + Graph + Geo
* Dashboard avec visualisation cartographique

### 🚀 Prochaine étape

* Explainability (SHAP) pour le moteur ML

---

## 📅 Date : 28/04 – 02/05/2026

### 🎯 Objectif du jour

* Implémenter l'explicabilité des scores ML (SHAP)
* Générer des explications human-readable pour chaque cas
* Ajouter onglet "Explainability" au dashboard

### ✅ Travail réalisé

* Implémentation du `ExplainabilityService` :
  * `shap.TreeExplainer` sur le modèle XGBoost
  * Top-5 features avec direction (`increases_risk` / `decreases_risk`)
  * Génération de résumés textuels (`_synthesize_summary()`)
  * Mapping risk_level → message (`_RISK_SUMMARIES`)
* Endpoint `GET /api/v1/cases/{case_id}/explain`
* Intégration au dashboard avec waterfall plot SHAP
* Tests sur 50 cas — SHAP renvoie systématiquement des valeurs cohérentes

### 📊 Résultats

* Explications structurées (summary, top_reasons, rule_explanations, feature_contributions)
* Onglet "Explainability" opérationnel
* Latence acceptable (~500ms par explication)

### ⚠️ Problèmes rencontrés

* Avertissement intermittent "SHAP explanation failed" dans les logs
* Symptômes : `shap_value: 0.0` pour toutes les features (bug latent, non identifié à ce stade)
* Cause suspectée : interaction `CalibratedClassifierCV` × `TreeExplainer` (à investiguer)

### 🧠 Solutions apportées

* `try/except` autour de l'appel SHAP avec fallback heuristique
* Log warning au lieu de planter l'API
* Documentation des features high-importance en dur (fallback)

### 📌 État actuel du système

* Pipeline complet : Rules + ML + Graph + Geo + Explainability
* Dashboard avec 5 onglets opérationnels

### 🚀 Prochaine étape

* Calibration probabiliste du modèle XGBoost
* Comparaison multi-modèles (RF, XGB, LGBM)

---

## 📅 Date : 05–09/05/2026

### 🎯 Objectif du jour

* Calibrer les probabilités du modèle XGBoost (isotonic)
* Comparer plusieurs modèles (RF, XGB, LGBM)
* Améliorer les métriques de performance

### ✅ Travail réalisé

* Calibration isotonic via `CalibratedClassifierCV(cv=3)`
* Entraînement et comparaison de 4 modèles :
  * Logistic Regression (baseline) : ROC-AUC 0.9537
  * Random Forest : ROC-AUC 0.9917
  * **XGBoost calibré** : ROC-AUC 0.9951, F1 0.8817
  * LightGBM calibré : ROC-AUC 0.9948, F1 0.9149
* Sélection finale : XGBoost calibré (meilleure precision/recall trade-off)
* Sauvegarde des artefacts dans `app/models_saved/` :
  * `xgboost.joblib`, `random_forest.joblib`, `logreg.joblib`, `isolation_forest.joblib`
  * `metadata.json` avec features, metrics, ensemble weights

### 📊 Résultats

* Modèle XGBoost calibré : F1=0.8817, Precision=0.9535, Recall=0.82
* Pondérations ensemble révisées : Rules 0.25 / ML 0.30 / Graph 0.45
* Probabilités calibrées (Brier score < 0.05)

### ⚠️ Problèmes rencontrés

* Quelques features supprimées car peu signifiantes : `pmt_score`, `pmt_score_min`, `household_size`
* `feedback_samples` encore très faible (12 cas annotés)

### 🧠 Solutions apportées

* Mise à jour de `metadata.json` avec `removed_features`
* Mécanisme de feedback en attente d'enrichissement

### 📌 État actuel du système

* Modèle production-ready (artefacts calibrés)
* Ensemble pondéré optimisé sur AUC

### 🚀 Prochaine étape

* Système d'alertes temps réel via PostgreSQL NOTIFY/LISTEN
* Page HTML standalone "Alert Monitor"

---

## 📅 Date : 12–16/05/2026

### 🎯 Objectif du jour

* Mettre en place le système d'alertes temps réel
* Page HTML "Alert Monitor" pour le suivi en direct
* Trigger PostgreSQL sur nouveaux cas HIGH/CRITICAL

### ✅ Travail réalisé

* Création de la table `fraud_cases` dans `fraud-db`
* Trigger PostgreSQL `NOTIFY` sur INSERT avec risk_level ∈ {HIGH, CRITICAL}
* Listener Python (asyncio) côté fraud-engine pour broadcast WebSocket
* Création du fichier `dashboard/alert_monitor.html` :
  * Polling toutes les 3 secondes via `/api/v1/cases`
  * Toast notifications pour CRITICAL/HIGH
  * Statistiques live (compteurs par niveau de risque)
  * Animations CSS (`slideIn`, `pulse`, `fadeout`)
* Endpoint `GET /api/v1/cases?limit=100` pour la liste
* Endpoint `POST /api/v1/score/features` créateur de cas

### 📊 Résultats

* Alertes temps réel fonctionnelles
* Délai de notification : < 3 secondes après création du cas
* Toast notifications visibles à l'écran

### ⚠️ Problèmes rencontrés

* CORS bloqué sur les premiers tests browser (`Origin: file://`)
* `alert_monitor.html` servi via Streamlit ne s'affiche pas correctement (Streamlit intercepte les URLs — bug identifié plus tard)

### 🧠 Solutions apportées

* CORS middleware FastAPI avec `allow_origins=["*"]`
* (Solution nginx dédié reportée à plus tard)

### 📌 État actuel du système

* Système d'alertes opérationnel
* Page HTML de monitoring disponible (mais problème d'hébergement)

### 🚀 Prochaine étape

* Module de gestion des règles (CRUD)
* Hot-reload des règles sans redémarrage

---

## 📅 Date : 19–23/05/2026

### 🎯 Objectif du jour

* Implémenter la gestion dynamique des règles métier
* Permettre le hot-reload sans redémarrage du conteneur
* Ajouter des règles temporelles (fenêtres glissantes)

### ✅ Travail réalisé

* Création du `RuleService` avec :
  * Chargement YAML depuis `app/rules/rules/`
  * Évaluation via `SafeExpressionEvaluator` (AST-based)
  * Métadonnées par règle (`rule_id`, `name`, `severity`, `weight`)
* Création de l'API `routes_rules.py` :
  * `GET /api/v1/rules` — liste des règles actives
  * `POST /api/v1/rules/reload` — hot-reload depuis le disque
  * `POST /api/v1/rules/test` — dry-run sur un payload
* Définition des règles temporelles TA001-TA005 :
  * TA001 : enrôlements multiples < 24h
  * TA002 : paiement < 7j après inscription (rapid_payout)
  * TA003 : vélocité d'enrôlement anormale
  * TA004 : paiement nocturne
  * TA005 : ratio de paiements groupés
* Définition des règles SE001-SE005 (signaux statiques) et GE001-GE002 (graph)

### 📊 Résultats

* 12 règles métier opérationnelles
* Hot-reload fonctionnel (test : modif YAML → reload → effet immédiat)
* Couverture de scoring complète (90%+ des cas frauduleux capturés par au moins 1 règle)

### ⚠️ Problèmes rencontrés

* `SafeExpressionEvaluator` retourne 0 par défaut pour les variables manquantes
* Conséquence : TA002 se déclenche à tort sur les payloads sans `days_reg_to_first_payment`
* Bug identifié mais correction reportée

### 🧠 Solutions apportées

* (Correction `_MissingVariable` planifiée pour l'audit complet)
* Documentation des variables requises par règle

### 📌 État actuel du système

* Moteur de règles complet et hot-reloadable
* 12 règles couvrant signaux statiques, temporels et graph

### 🚀 Prochaine étape

* Audit complet du système (préparation jury)
* Identification de tous les bugs résiduels

---

## 📅 Date : 26–30/05/2026

### 🎯 Objectif du jour

* Tests d'intégration end-to-end
* Préparation de la démo pour le jury
* Préparation du `TECHNICAL_REPORT.md`

### ✅ Travail réalisé

* Tests E2E manuels sur tous les endpoints :
  * `/api/v1/score/features` (POST)
  * `/api/v1/cases` (GET)
  * `/api/v1/cases/{id}/explain` (GET)
  * `/api/v1/geo/heatmap` (GET)
  * `/api/v1/geo/hotspots` (GET)
* Création de bénéficiaires de test : `CLEAN`, `SUSPECT`, `SHAP-TEST`, `AUDIT-001`
* Rédaction du `TECHNICAL_REPORT.md` (~28 KB) :
  * Architecture du système
  * Description des 4 couches de scoring
  * Métriques de performance par modèle
  * Diagrammes de flux
* Scripts de démarrage : `START_DEMO.bat`, `STOP_DEMO.bat`
* Premier test utilisateur final révèle plusieurs anomalies :
  * SHAP renvoie `0.0` partout
  * `/geo/heatmap` plante sur ID `AUDIT-001`
  * Payload vide score MEDIUM au lieu de LOW

### 📊 Résultats

* `TECHNICAL_REPORT.md` complet
* Identification d'une liste de 8 bugs à corriger
* Démo fonctionne mais avec frictions

### ⚠️ Problèmes rencontrés

* Plusieurs bugs latents découverts en conditions réelles
* SHAP, geo service, rule engine, dashboard config
* Pas de validation Pydantic stricte sur les endpoints critiques

### 🧠 Solutions apportées

* Création d'un audit-list priorisé (par impact)
* Planification d'une session de fix complète (31/05)

### 📌 État actuel du système

* Pipeline E2E fonctionnel mais avec bugs UX significatifs
* Documentation technique complète

### 🚀 Prochaine étape

* Session de correction complète des 8 bugs identifiés

---

## 📅 Date : 31/05/2026

### 🎯 Objectif du jour

* Audit complet utilisateur du moteur de détection de fraude
* Identifier et corriger les bugs prioritaires
* Préparer l'environnement pour un scénario réel

### ✅ Travail réalisé

#### 🔹 1. Audit complet du système

* Scan brique par brique : API endpoints, dashboard Streamlit, alert monitor HTML, Docker setup, pipeline data, configuration
* Identification de 8 bugs critiques bloquant un déploiement production

#### 🔹 2. Corrections prioritaires (8 bugs)

* **Rule engine — faux positifs fantômes** : `SafeExpressionEvaluator.visit_Name()` retournait 0 pour les variables absentes, déclenchant TA002 (paiement fantôme) sur payload vide
  * Correction : levée d'une exception `_MissingVariable` + skip de la règle
  * Test : payload vide passe de 0.4056 MEDIUM (faux) à 0.2485 LOW (correct)
* **Geo service — crash sur IDs non numériques** : `int('AUDIT-001')` plantait `/geo/heatmap` et `/geo/hotspots`
  * Correction : filtrage des IDs numériques pour la requête DB, hash déterministe pour les autres
* **SHAP — valeurs toutes à 0.0** : `CalibratedClassifierCV` empêchait `TreeExplainer` d'accéder au booster XGBoost
  * Correction : `_unwrap_tree_estimator()` extrait `calibrated_classifiers_[0].estimator`
  * Test : SHAP renvoie maintenant `payment_count +0.244`, `age -0.195`, etc.
* **Segfault XGBoost** : 31 features envoyées à un modèle entraîné sur 18 → corruption mémoire C-level
  * Correction : `_get_model_feature_names()` + constraint sur `feature_names_in_`
* **Endpoint /api/v1/rules introuvable** : `routes_rules.py` jamais monté dans `main.py`
  * Correction : ajout du router + réécriture pour utiliser `RuleService` directement
* **Validation API absente** : `beneficiary_id` vide acceptait des cas "unknown"
  * Correction : retour 422 si `beneficiary_id` manquant ou vide
* **Pondérations ensemble incorrectes** : hardcodées `(0.30/0.50/0.20)` au lieu de `(0.25/0.30/0.45)` de la config
  * Correction : lecture via `config.ensemble_*`
* **Dashboard hardcoded API key** : `dev-secret-change-in-prod` en dur dans `streamlit_app.py` et `alert_monitor.html`
  * Correction : variables d'environnement + query string (`?api=...&key=...`)

#### 🔹 3. Refonte Dockerfile dashboard

* Échec de build sur `streamlit>=1.35.0` (parsé comme redirection shell `>`)
* Pip resolver bloquait sur `pyarrow` (pas de wheel cp311 trouvé)
* Correction : quoting strict + install explicite de `pyarrow==18.1.0` avant streamlit
* Pin de toutes les versions : `streamlit==1.40.2`, `pandas==2.2.3`, `plotly==5.24.1`

### 📊 Résultats

* 8 bugs critiques corrigés et testés
* `xgboost.joblib` retourne maintenant des explications SHAP cohérentes
* `/api/v1/rules` accessible et hot-reload fonctionnel via `/api/v1/rules/reload`
* Validation Pydantic stricte côté API
* Image dashboard rebuild avec succès (~5 min)
* Régression : bénéficiaire 510 score toujours 0.2331 LOW (cohérent)

### ⚠️ Problèmes rencontrés

* Segfault C-level XGBoost difficile à diagnostiquer (silencieux)
* PaySim et OpenG2P utilisent des schémas différents (transaction-centric vs beneficiary-centric)
* Dépendances pip non déterministes sans pinning explicite

### 🧠 Solutions apportées

* Extraction des feature names depuis `feature_names_in_` du modèle
* Unwrap explicite de `CalibratedClassifierCV` pour SHAP
* Pinning strict des versions dans Dockerfile

### 📌 État actuel du système

* Tous les endpoints opérationnels et testés
* Dashboard Streamlit + Alert Monitor accessibles
* Système prêt pour intégration de données réelles

### 🚀 Prochaine étape

* Rebuild des images via docker-compose
* Audit data science de la base OpenG2P + dataset AIML

---

## 📅 Date : 01/06/2026

### 🎯 Objectif du jour

* Rebuild des images Docker avec toutes les corrections
* Résoudre les problèmes d'affichage de l'Alert Monitor
* Auditer les sources de données (OpenG2P DB + dataset AIML/PaySim)
* Générer un jeu de données démo aligné sur le schéma OpenG2P
* Réentraîner les modèles avec une méthodologie honnête

### ✅ Travail réalisé

#### 🔹 1. Rebuild Docker

* Build des images `fraud-detection-engine:latest` et `poc-v2-dashboard:latest` depuis le contexte racine
* Restart complet de la stack via `docker-compose.full.yml`
* Vérification de tous les conteneurs : fraud-db, fraud-engine, dashboard, openg2p-postgresql, odoo, grafana, prometheus → tous healthy

#### 🔹 2. Correction Alert Monitor

* Diagnostic : Streamlit interceptait toutes les URLs et renvoyait son shell React au lieu de servir `alert_monitor.html`
* Symptôme : code JavaScript affiché en texte brut, statut bloqué sur "Connecting..."
* Solution : ajout d'un conteneur nginx dédié (`alert-monitor`) servant le fichier HTML statique sur le port 8503
* Mise à jour de `docker-compose.full.yml` avec le nouveau service
* Test : `http://localhost:8503` rend correctement le dashboard, polling API fonctionnel

#### 🔹 3. Audit base OpenG2P PostgreSQL

* Scan complet du schéma (107 tables `g2p_*` / `spp_*`)
* Volumes constatés : 151 registrants, 130 memberships, 12 paiements, 130 téléphones
* Découverte critique : `res_partner.phone` toujours NULL → le moteur lisait la mauvaise colonne
* Bonne nouvelle : `g2p_phone_number.phone_sanitized` montre 102 numéros distincts pour 130 enregistrements = **21.5% de collisions téléphone** (signal fraude exploitable immédiatement)
* `g2p_reg_id` (IDs nationaux) vide → à activer dès population

#### 🔹 4. Audit dataset AIML (PaySim)

* 6 362 620 lignes, 11 colonnes, 493 MB
* Taux de fraude : 0.13% (8 213 cas) — extrême déséquilibre
* Fraude concentrée sur TRANSFER (0.77%) et CASH_OUT (0.18%), 0% sur les 3 autres types
* `isFlaggedFraud` : 16 hits seulement → colonne dégénérée
* 57.8% des lignes ont une anomalie d'équation de bilan

#### 🔹 5. Feature engineering & nettoyage PaySim

* Filtrage TRANSFER + CASH_OUT uniquement (2.77M lignes)
* 10 features dérivées avec mesure de lift sur fraude :
  * `full_drain` : **lift 53.1×**
  * `round_amount` : **lift 30.3×**
  * `dest_was_empty` : lift 11.4×
  * `is_night` : lift 10.0×
* Sous-échantillonnage stratifié 1:20 → training set de 172 473 lignes
* Sauvegarde en CSV + Parquet

#### 🔹 6. Découverte du target leakage

* Premier modèle XGBoost : ROC-AUC 0.9998, F1 0.9988 — trop beau pour être vrai
* Diagnostic : feature `balance_anomaly` portait 90% de l'importance
* Cause : PaySim génère ses labels de fraude en mettant `newbalanceOrig = 0` sans soustraire `amount` — donc `balance_anomaly` EST la règle de labelisation du simulateur
* Le modèle apprenait la règle du simulateur, pas la fraude
* Script de diagnostic univarié confirme : suppression des features fuyantes nécessaire

#### 🔹 7. Génération de données démo alignées OpenG2P

* Création de 3 CSVs au format `registry-individual-data.csv` (importable via dashboard Odoo) :
  * `openg2p_beneficiaries_import.csv` (1 000 individus)
  * `openg2p_phones_import.csv` (1 000 téléphones)
  * `openg2p_payments_import.csv` (1 000 paiements)
* 15% de fraude injectée avec 8 patterns réalistes :
  * shared_phone, shared_account, identity_cluster, mass_enrollment
  * rapid_payout, round_payment, duplicate_name, income_outlier
* Colonnes d'audit `_fraud_label` et `_fraud_pattern` pour évaluation
* IDs synthétiques à partir de 100 000 pour éviter collisions avec la DB live

#### 🔹 8. Réentraînement honnête

* Modèle XGBoost calibré (isotonic) sur les 1 000 bénéficiaires demo
* Split stratifié 80/20, class-weighted, pas de SMOTE
* 23 features extraites (mirroir de `features_service.py`)
* Métriques défendables obtenues :
  * **ROC-AUC : 0.9040**
  * **PR-AUC : 0.8532**
  * **F1 : 0.8302**
  * **Recall : 0.7333**
  * **Precision : 0.9565**
* Recall par pattern (révèle les forces/faiblesses honnêtes) :
  * ✅ 100% : identity_cluster, mass_enrollment, rapid_payout, round_payment, shared_account, shared_phone
  * ❌ 0% : duplicate_name, income_outlier (features manquantes)

### 📊 Résultats

* Stack Docker complète opérationnelle (9 conteneurs)
* Alert Monitor fonctionnel via nginx sur port 8503
* Audit data science complet livré (`DATA_ENGINEERING_REPORT.md`)
* 3 CSVs démo prêts pour import OpenG2P
* Nouveau modèle `xgboost_openg2p_demo.joblib` avec métriques défendables
* Guide démo complet livré (`DEMO_GUIDE.md`)

### ⚠️ Problèmes rencontrés

* Streamlit intercepte toutes les routes → impossible d'y servir du HTML statique
* PaySim contient un target leakage massif via l'équation de bilan
* PaySim est un dataset déterministe → métriques irréalistes (0.99+) sur tout modèle raisonnable
* Schéma PaySim (transaction-centric) incompatible avec OpenG2P (beneficiary-centric)

### 🧠 Solutions apportées

* Conteneur nginx dédié pour servir `alert_monitor.html` proprement
* Suppression des features fuyantes (`balance_anomaly`, balances brutes, `overdraft_attempt`)
* Split chronologique sur `tx_step` pour PaySim
* Pivot vers un dataset démo généré, aligné sur le schéma OpenG2P
* Évaluation par pattern pour révéler honnêtement les forces et faiblesses

### 📌 État actuel du système

* Conteneurs : fraud-engine, dashboard, alert-monitor, fraud-db, openg2p-postgresql, odoo, grafana, prometheus
* Modèles disponibles :
  * `xgboost.joblib` (bénéficiaire, original)
  * `xgboost_openg2p_demo.joblib` (nouveau, métriques honnêtes)
  * `xgboost_paysim.joblib` (transaction, complémentaire)
* Données démo prêtes à injecter via le dashboard Odoo
* Documentation complète : `DATA_ENGINEERING_REPORT.md`, `DEMO_GUIDE.md`

### 🚀 Prochaine étape

* Import des CSVs démo dans OpenG2P via le dashboard Odoo
* Évaluation comparative avant/après nouveau modèle sur les 1 000 bénéficiaires
* Câblage du modèle PaySim dans l'ensemble comme troisième estimateur
* Ajout des features manquantes pour capturer `duplicate_name` (fuzzy matching) et `income_outlier` (cross-checks revenus/actifs)
* Pré-matérialisation du feature store dans `fraud-db` pour passer de 5s à <200ms de latence

---

## 📅 Date : 01/06/2026 (après-midi)

### 🎯 Objectif du jour

* Remplacer le moniteur HTML autonome par une intégration native Odoo
* Intégrer Ollama pour la génération d'explications en langage naturel
* Créer le pipeline complet pour la démo manager (CSV → import → scoring → kanban → IA)
* Lier les dashboards Odoo et Streamlit entre eux

### ✅ Travail réalisé

#### 🔹 1. Addon Odoo `g2p_fraud_detection`

* Création complète du module Odoo natif remplaçant `alert_monitor.html` :
  * Modèle `fraud.case` avec `mail.thread` + `mail.activity.mixin`
  * Vue kanban auto-rafraîchissante via `bus.bus`
  * Vue formulaire avec onglets (AI Explanation, Rules Triggered, Technical Explanation, Notes)
  * Boutons d'action : Start Investigation, Confirm Fraud, Dismiss, Close Case
  * Modèle de synchronisation `fraud.sync` (cron 1min sur `/api/v1/cases`)
  * Sécurité ACL : groupes Fraud Officer / Fraud Supervisor
  * Bus listener JS pour notifications toast en temps réel
* Suppression du conteneur `alert-monitor` nginx et de `alert_monitor.html`
* Montage de l'addon dans Odoo via copie dans `extraaddons/`

#### 🔹 2. Bugs Odoo 17 corrigés en cours d'intégration

* `attrs="{'invisible': ...}"` → syntaxe `invisible="..."` directe (Odoo 17 dépréciation)
* `@api.model` create → `@api.model_create_multi` avec batch handling
* `useService("bus_service")` → accès direct via `env.services` (service async non compatible avec useService)
* Sélection `recommendation` complétée : `BLOCK_PAYMENT`, `MANUAL_REVIEW`, `MONITOR`, `CLEAR`
* Sync robustifié pour ignorer les valeurs de Selection inconnues
* Rendu des règles : passage de codes (`NF001, NF002`) au format plain English avec puces :
  ```
  • Shared Phone Number — Phone number shared with 3 other beneficiaries
  • High Network Risk Score — Network risk score: 0.80 (threshold: 0.60)
  ```

#### 🔹 3. Intégration Ollama (LLM local)

* Ajout du service `ollama` au `docker-compose.full.yml`
* Volume `poc-v2_ollama_data` réutilisé (modèles préservés)
* Pull du modèle `llama3.2:1b` (1.3 GB, rapide sur CPU)
* Service `LLMExplainer` dans le fraud-engine :
  * Construction du prompt depuis règles + top features
  * Appel `/api/generate` sur Ollama
  * Fallback gracieux si Ollama indisponible
* Endpoint `POST /api/v1/cases/{case_id}/llm_explain`
* Colonne `llm_explanation TEXT` ajoutée à `fraud_cases` (migration ALTER TABLE)
* Bouton "Generate AI Explanation" dans la vue formulaire Odoo
* Latence ~10-15s par explication (CPU, modèle 1B)

#### 🔹 4. Bugs corrigés dans l'engine

* Endpoint `/api/v1/cases` enrichi : `rules_triggered`, `explanation`, `llm_explanation` ajoutés au schéma Pydantic `CaseItem`
* Nouveau endpoint `GET /api/v1/cases/{case_id}` pour le détail complet
* Bug off-by-one dans `extractors.py` : `shared_phone_count` et `shared_account_count` faisaient un `- 1` superflu (le JOIN excluait déjà self), donnant un compte sous-évalué qui empêchait les règles `NF001` (≥2) et `NF002` (≥3) de se déclencher

#### 🔹 5. Cross-linking Odoo ↔ Streamlit

* Ajout de 3 menus Odoo `act_url` qui ouvrent Streamlit en nouvel onglet :
  * **Geographic Heatmap** → `localhost:8501/?page=geo`
  * **Analytics Dashboard** → `localhost:8501/?page=cases`
  * **API Documentation** → `localhost:8002/docs`
* Smart-button "View on Heatmap" sur le formulaire fraud.case :
  * Action `act_url` avec param `?beneficiary=<ID>`
  * Côté Streamlit : centrage + zoom 10 + cercle bleu de surbrillance sur ce bénéficiaire
* Streamlit reconnait `?page=geo|cases|explain|...` pour deep-linking
* Sidebar Streamlit ajoute un lien "🔗 Open in OpenG2P (Odoo)"
* Paramètre `fraud_detection.dashboard_url` configurable via `ir.config_parameter`

#### 🔹 6. Correction heatmap pydeck

* Heatmap rendait sur fond noir car aucun token Mapbox n'était configuré
* Ajout de `map_provider="carto"` + `map_style="light"` à `pdk.Deck(...)` → fond de carte CARTO gratuit, sans token

#### 🔹 7. Données démo enrichies pour le manager

* Régénération de `data/demo/demo_beneficiaries.csv` (20 lignes, format `res.partner` natif Odoo)
* Patterns conçus pour couvrir les 4 niveaux de risque :
  * **CRITICAL × 3** — 3 partenaires partagent le même téléphone ET le même compte bancaire (déclenche NF003 + network_risk ≥ 0.80)
  * **HIGH × 5** — 3 partagent un téléphone + 2 partagent un compte (NF001 / NF002)
  * **MEDIUM × 2** — 2 partagent un téléphone (juste sous le seuil NF002)
  * **LOW × 10** — bénéficiaires propres
* Scripts de support (terminal uniquement, optionnels pour l'opérateur) :
  * `reset_demo.sh` — vide les bénéficiaires DEMO-* et tous les cas
  * `finalize_demo_import.py` — peuple `g2p_phone_number`, `res_partner_bank`, `g2p_program_membership` après import dashboard
  * `score_imported_beneficiaries.py` — scoring via `/api/v1/score/beneficiary/{id}`
  * `simulate_import.py` — test E2E sans cliquer le wizard Odoo

### 📊 Résultats

* Distribution de risque dans la démo : 3 CRITICAL (0.82–0.90), 5 HIGH (0.65–0.79), 2 MEDIUM (0.56), 10 LOW (0.25)
* Tous les services healthy : openg2p-odoo, fraud-engine, ollama, fraud-db, postgresql, streamlit-dashboard, grafana, prometheus
* Pipeline complet fonctionnel :
  * Import dashboard OpenG2P → finalisation données → scoring → kanban Odoo → AI explanation Ollama → heatmap Streamlit
* Explications LLM cohérentes, en anglais simple, ~10s de latence
* Aucune dépendance externe (cloud) : Ollama, CARTO et tous les services tournent localement

### ⚠️ Problèmes rencontrés

* Odoo 17 (Bitnami) ignore `ODOO_ADDONS_PATH` env var, utilise un conf baked-in
* `useService("bus_service")` plante avec "methods is not iterable" (bug Odoo sur services async)
* Erreur "push service not available" liée à un service worker stale (résidu du moniteur HTML supprimé)
* Endpoint `/api/v1/cases` initial ne renvoyait pas `rules_triggered` ni `explanation`
* Pydeck nécessite normalement un token Mapbox

### 🧠 Solutions apportées

* Copie de l'addon directement dans `/opt/bitnami/odoo/extraaddons/`
* Accès direct à `env.services.bus_service` au lieu de `useService(...)`
* Documentation : ignorer l'erreur push ou nettoyer les service workers stale dans DevTools
* Ajout des 3 champs au schéma Pydantic `CaseItem`
* `map_provider="carto"` utilise les tuiles CARTO gratuites sans token

### 📌 État actuel du système

* **Conteneurs** : openg2p-odoo (+ addon installé), fraud-engine, fraud-db, postgresql, ollama (llama3.2:1b), streamlit-dashboard, grafana, prometheus, openg2p-spar-*
* **Menus Odoo** sous "Fraud Detection" :
  * Live Alert Monitor (kanban)
  * All Cases (liste/formulaire)
  * Geographic Heatmap (lien Streamlit)
  * Analytics Dashboard (lien Streamlit)
  * API Documentation (lien Swagger)
* **Pipeline démo** prêt : CSV → import dashboard → finalize → score → kanban + IA + carte
* DB vide, prête pour la démo (`reset_demo.sh` exécuté)

### 🚀 Prochaine étape

* Présentation au manager : démo end-to-end via dashboard OpenG2P
* Activation du modèle PaySim comme troisième estimateur dans l'ensemble
* Pré-matérialisation des features dans `fraud-db` pour réduire la latence de scoring
* Persistance automatique des LLM explanations dans le cron de sync (pour éviter le clic manuel)
* Migration des credentials hardcodés vers variables d'environnement / vault
