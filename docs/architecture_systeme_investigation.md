# Système d'investigation documentaire multimodal on-premise

## 1. Objectifs et principes directeurs
- **Exactitude et traçabilité** : chaque élément extrait doit conserver ses références (document, page, boîte de délimitation, horodatage).
- **Stack 100 % on-premise** : uniquement des composants open source ou déjà disponibles localement (LLM, embeddings).
- **Résilience et traitements longs** : orchestrations idempotentes, reprise sur incident, journalisation complète des versions et paramètres.
- **Réduction des biais et hallucinations** : filtrage par clusters, décodage contraint, appels au LLM uniquement avec preuves vérifiables, refus explicite en l'absence d'évidence.

## 2. Architecture globale
```
Serveur de fichiers  -->  Pipeline d'ingestion (Airflow/Prefect)
                               |
                               v
                      Stockage brut (MinIO/ceph)
                               |
                               v
     Parsing multimodal (GROBID + OCR + LayoutParser + detectron2)
                               |
                               v
   Indexation unifiée (PostgreSQL + pgvector + Elasticsearch/OpenSearch)
                               |
                               v
      Knowledge Graph (Neo4j ou Azure Cosmos Gremlin on-prem? -> Neo4j) + GeoSPARQL/PostGIS
                               |
                               v
  Orchestrateur RAG hybride (LangChain local + agent de raisonnement itératif)
                               |
                               v
Interface web (FastAPI + Vue.js) + Observabilité (Prometheus/Grafana)
```

## 3. Pipeline d'ingestion
### 3.1 Déclenchement
- **Surveillance** : service de watch (inotifywait ou fswatch) capturant les nouveaux PDF déposés sur le serveur.
- **Orchestration** : Prefect 2.x (léger, Python, support idempotence) ou Apache Airflow (si besoin scheduling complexe). Choix : Prefect pour réduire la stack.

### 3.2 Étapes principales
1. **Enregistrement initial** : copie du fichier dans stockage objet on-prem (MinIO) avec versioning + métadonnées (hash, taille, horodatage, source).
2. **Déduplication précoce** :
   - Texte : MinHash/SimHash sur plain text extrait grossièrement via pdfminer.six.
   - Images/cartes : pHash sur images extraites rapidement via pdfimages.
   - Si doublon exact ou near-duplicate, création d'un lien logique plutôt que réingestion.
3. **Prétraitement** : normalisation PDF (QPDF), réparation structure, séparation si dossier multi-docs.
4. **Parsing multimodal** :
   - **Texte structuré** : GROBID pour métadonnées + segmentation logique.
   - **Layout** : pdfplumber + LayoutParser (modèle détecteur basé sur Detectron2 ou PaddleOCR) pour identifier blocs (texte, tableaux, figures, cartes).
   - **OCR** : Tesseract + modèles spécifiques (fr, multi-langues). Pour cartes/schémas, utiliser OCR orienté (PaddleOCR) + orientation detection.
   - **Images** : extraction via `pdfimages`, classification type (photo/carte/diagramme) via modèle Vision (clip-like) local.
   - **Cartes/Géométrie** : pipeline spécifique utilisant `mapdetect` ou modèle CNN pour reconnaître cartes; extraction de légendes; vectorisation via `Potrace` + heuristiques.
   - **Tableaux** : `Camelot`/`Tabula` + `DeepDeSRT` pour structure.
5. **Segmentation layout-aware** : chaque élément -> segment avec doc_id, page, bbox, type, hiérarchie logique (section, paragraphe, tableau cell). Stockage en JSON.
6. **OCR correction** : passage par modèle de correction (seq2seq) local ou LLM en mode constrained decoding (utiliser FST ou dictionnaires).

### 3.3 Journalisation
- `Prefect` stocke état des tâches, logs détaillés.
- Tous les artefacts + configs versionnés dans `MLflow` ou `DVC` (local) pour audits.

## 4. Indexation et stockage
### 4.1 Schéma de données
- **PostgreSQL + PostGIS + pgvector** comme entrepôt central.
- Tables principales :
  - `documents` : métadonnées globales, version, hash, statut.
  - `segments` : type, texte, bbox, page, embeddings texte (pgvector), références.
  - `media` : images/figures vectorisées, embeddings visuels.
  - `entities` : entités extraites, type, provenance segment/page.
  - `relations` : relations et événements, attributs temporels/spatiaux.
  - `clusters` : thématiques, version campagne, paramètres.
  - `tasks` : suivis des investigations, journaux, décisions agent.

### 4.2 Index texte + recherche lexicale
- `OpenSearch` (fork open source d’Elasticsearch) déployé on-prem.
- Index combinant tokens (BM25), champs analytiques (langue), n-grams.
- Synchronisation via workers (Kafka/Pulsar ou simple queue `RabbitMQ`). Choix minimaliste : `Redis Streams`.

### 4.3 Vecteurs
- Embeddings texte via API interne -> stockés dans `pgvector` (extension).
- Embeddings image/cartes via modèle CLIP-like local -> stockage `pgvector` (dimension séparée).
- Index HNSW (pgvector `ivfflat`/`hnsw`).

### 4.4 Métadonnées spatio-temporelles
- PostGIS pour géométries (points, polygones), indexes `GIST`.
- Toutes les entités géographiques associées à segments -> relation `segment_geo`.

### 4.5 Stockage binaire
- MinIO pour segments binaires (images, pdf originaux, extraits), versioning activé.

## 5. Extraction d’information
### 5.1 NER et classification
- Modèles spaCy entraînés sur domaine + adaptation via fine-tuning (SimCSE/TSDAE pour embeddings). Option : `flair` pour PER/ORG/LOC.
- Cross-doc coref : pipeline `spaCy` + heuristiques (normalisation toponymes) + clustering (agglomératif) sur embeddings entité + règles (Jaro-Winkler, geohash).

### 5.2 Relations et événements
- Modèle transformer local (e.g. `DyGIE++` fine-tuné) ou `SpERT` pour relations.
- Extraction d'événements via `Pytorch Lightning` pipeline, reliant triggers, arguments.
- Ancrage temporel : extraction dates (HeidelTime offline) + normalisation (iso8601) + alignement à segments.

### 5.3 Géoparsing
- `spaCy` + `GeoText` + base toponymique locale (Geonames offline). Appariement via PostGIS (point-in-polygon, distances).
- Normalisation en `EPSG:4326`, stockage dans PostGIS.

### 5.4 Graphes et corrélation
- Construction d'un graphe dans `Neo4j` (ou `Memgraph` si besoin perfs). Noeuds = entités normalisées, événements. Arêtes = relations (avec attributs: poids, sources, temps).
- Versionnement par campagne : chaque run -> sous-graphe daté.

### 5.5 Détection d’outliers
- Features agrégées (embedding moyen, temporalité, densité). Modèles `IsolationForest`/`LOF` exécutés offline; résultats stockés table `outlier_signals`.

## 6. Clustering thématique et topic discovery
- **Pipeline** :
  1. Pour chaque campagne -> calcul embeddings (texte + image concaténés via weighted sum).
  2. Réduction dimensionnelle (UMAP) -> HDBSCAN pour clusters souples.
  3. Clusters utilisés pour filtrage retrieval.
  4. Auto-taxonomie via `BERTopic` (utilise UMAP + HDBSCAN + c-TF-IDF) pour labels.
  5. NMF sur TF-IDF comme alternative supervisée pour validation.
- Stockage : table `clusters` (id, label, membres, scores) + `cluster_versions`.
- Journalisation : seeds, hyperparams (min_cluster_size, min_samples, random_state) stockés dans `MLflow`.

## 7. Pipeline RAG hybride et agent de raisonnement
### 7.1 Pré-sélection
1. **Filtrage** : par clusters/topic, période, géographie, type média.
2. **kNN** : pgvector sur embeddings filtrés.
3. **Rerank** : BM25 (OpenSearch) + cross-encoder local (fine-tuné).

### 7.2 Génération de contexte
- Concaténation contrôlée (max tokens) avec métadonnées (doc/page/bbox) + extraits images -> conversion en description textuelle via vision-LLM interne.
- Contrôle de redondance via MinHash; priorité segments divers.

### 7.3 Agent de raisonnement
- Agent itératif (LangChain ou LlamaIndex) orchestrant appels :
  - **Outils** : recherche cluster, recherche vecteur, requêtes graphiques Neo4j, requêtes spatio-temporelles PostGIS, vision (captioning).
  - Chaque étape loggée (outil, input, output, segments utilisés).
- Décodage contraint : LLM paramétré température basse, top-p restreint, injection instructions d’ancrage.
- Vérification post-génération : script compare citations vs segments. Si mismatch => reformulation.
- Refus : si score confiance < seuil ou segments insuffisants -> message d’impossibilité avec justification.

### 7.4 Traçabilité
- Pour chaque réponse : stockage JSON (query, étapes agent, segments cités, version modèles, timestamp).
- `Elasticsearch` index `audits` pour recherches ultérieures.

## 8. Interface web
### 8.1 Back-end
- **FastAPI** exposant endpoints :
  - `/ingest/status`, `/query`, `/tasks/{id}`, `/reports/{id}`.
  - Websocket pour streaming état agent.
- Authentification : Keycloak on-prem ou `Authelia` + SSO (LDAP).
- Rate limiting : `fastapi-limiter` (Redis).

### 8.2 Front-end
- Framework Vue.js 3 + Vite (SPA). Modules :
  - Tableau de bord ingestion (statuts, logs Prefect).
  - Formulaire question/tâche (sélection période, zones, topics).
  - Vue conversationnelle de l’agent (étapes + citations cliquables).
  - Visualisation preuves :
    - visionneuse PDF (pdf.js) avec surlignage bbox.
    - Galerie images/cartes.
  - Graphes relationnels : `D3.js` ou `Cytoscape.js`.
  - Cartes : `MapLibre GL` + affichage GeoJSON.
- Export rapport PDF via `WeasyPrint`/`ReportLab` côté serveur.

### 8.3 Observabilité UI
- Collecte métriques via `Prometheus` + dashboard Grafana (CPU, latence, files d’attente).
- Journalisation front (audit trail) -> envoi vers backend (Elastic APM).

## 9. Stratégie de suppression contrôlée
- Après réponse finale validée, l’utilisateur peut déclencher suppression segments associés.
- Politique : marquer `documents.status = TO_DELETE` -> job batch supprime du stockage MinIO + index + graph + audit (sauf traces minimales anonymisées).
- Conformité : logs de suppression signés (hash).

## 10. Plan de déploiement on-premise
- Conteneurisation via Docker + orchestration `Kubernetes` (optionnel) ou `Nomad`/`Docker Compose` selon taille.
- Séparation réseaux :
  - VLAN ingestion (accès serveur fichiers).
  - VLAN traitement (LLM, DB).
  - VLAN interface utilisateurs (reverse proxy Nginx).
- Sécurité :
  - Vault pour secrets.
  - Audit : Wazuh ou auditd.
  - RBAC strict (PostgreSQL, Neo4j, MinIO).
- Sauvegardes : snapshots MinIO + dump PostgreSQL + export Neo4j (apoc) planifiés.

## 11. Gestion des campagnes et versionnement
- Chaque campagne d’investigation -> ID unique, configuration (plages temporelles, topics).
- Versionnement : `MLflow` projet par campagne (hyperparams, modèles, seeds).
- Index/clusters suffixés par `campaign_id`.
- Reprise : Prefect tasks résumées; en cas d’échec, redémarrage sur segments restants (statut `PENDING`, `DONE`, `FAILED`).

## 12. Gouvernance des modèles
- Registre modèles local (MLflow Model Registry).
- Protocoles d’évaluation périodique (précision NER, relations, alignement citations).
- Surveillance dérive : distribution embeddings, entropie clusters.

## 13. Plan de tests et validation
- Tests unitaires pipelines (pytest).
- Tests d’intégration ingestion->indexation (jeu de PDF synthétique).
- Tests de charge retrieval (Locust) et interface (Playwright).
- Audit manuel : vérification citations page-précises, alignement cartes.

## 14. Feuille de route d’implémentation (phases)
1. **Phase 0 – Infrastructure** : MinIO, PostgreSQL+PostGIS+pgvector, Prefect, FastAPI skeleton, UI basique.
2. **Phase 1 – Parsing & Indexation** : GROBID, OCR, LayoutParser, stockage segments, ingestion automatique.
3. **Phase 2 – NER/Relations/Graph** : extraction entités, graphe Neo4j, géoparsing.
4. **Phase 3 – Clustering avancé & topic discovery** : HDBSCAN, BERTopic, déduplication complète.
5. **Phase 4 – Agent RAG hybride** : outillage agent, citations strictes, scoring confiance.
6. **Phase 5 – Interface avancée + rapports** : visualisations, export, gestion suppression.
7. **Phase continue – Optimisations** : adaptation embeddings, détection anomalies, monitoring.

## 15. Contraintes et mitigations
- **Qualité variable PDF** : pipeline multi-OCR, heuristiques correction, escalade humaine si OCR < seuil.
- **Traitements longs** : exécution batch nocturne, partitions par campagne, priorisation.
- **Biais/hallucinations** :
  - Contextualisation stricte.
  - Agent vérifie citations via script.
  - Score confiance -> refus possible.
- **Sécurité** : isolement réseau, scans vulnérabilités, IAM minimal.

## 16. Indicateurs de performance clés (KPIs)
- Taux d’extraction entités exactes (>85%).
- Couverture citations : 100 % des réponses citées.
- Temps moyen ingestion doc (objectif < 10 min/doc).
- Réduction duplication (>90%).
- Satisfaction analystes (feedback). 

