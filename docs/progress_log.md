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

