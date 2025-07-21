import time
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import numpy as np
from sqlalchemy import create_engine, text, inspect
from flask import Flask, request, jsonify
import uuid
import traceback

app = Flask(__name__)

try:
    model_mini = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    model_distil = SentenceTransformer('distiluse-base-multilingual-cased-v2')
    print("Modèles SentenceTransformer chargés avec succès.")
except Exception as e:
    print(f"Erreur critique lors du chargement des modèles SentenceTransformer: {e}\n{traceback.format_exc()}")
    model_mini = None
    model_distil = None

poids_mini = 0.30
poids_distil = 0.70

# Paramètres de connexion MySQL
MYSQL_CONN_PARAMS = {
    "host": "51.77.213.75",
    "port": 3307,
    "user": "ai",
    "password": "XEVrQHF2PzXRE2UKmYKs",
    "database": "interfaces"
}

def get_snowflake_engine():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_CONN_PARAMS['user']}:{MYSQL_CONN_PARAMS['password']}@"
            f"{MYSQL_CONN_PARAMS['host']}:{MYSQL_CONN_PARAMS['port']}/{MYSQL_CONN_PARAMS['database']}"
        )
        with engine.connect() as conn:
            print("Connexion MySQL établie avec succès.")
        return engine
    except Exception as e:
        print(f"Erreur lors de la création du moteur MySQL: {e}\n{traceback.format_exc()}")
        raise

def get_table_columns(engine, table_name):
    try:
        with engine.connect() as connection:
            inspector = inspect(engine)
            columns = [col['name'].lower() for col in inspector.get_columns(table_name)]
            print(f"Colonnes de la table {table_name}: {columns}")
            return columns
    except Exception as e:
        print(f"Erreur lors de la récupération des colonnes de {table_name}: {e}\n{traceback.format_exc()}")
        return []

def fetch_latest_demande(engine, table_name, id_to_match=None):
    try:
        columns = get_table_columns(engine, table_name)
        query = f"SELECT * FROM `{table_name}`"
        params = {}
        if id_to_match and 'id' in columns:
            query += " WHERE ID = :id"
            params = {'id': id_to_match}
        query += " ORDER BY ID DESC LIMIT 1"
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            row = result.fetchone()
            if row:
                df = pd.DataFrame([row], columns=[col.lower() for col in result.keys()])
                print(f"Dernière demande récupérée de {table_name} pour id={id_to_match}: {df.to_dict()}")
                return df
            else:
                print(f"Aucune demande trouvée dans {table_name} pour id={id_to_match}.")
                return pd.DataFrame()
    except Exception as e:
        print(f"Erreur lors de la récupération de la dernière demande de {table_name}: {e}\n{traceback.format_exc()}")
        return pd.DataFrame()

def fetch_candidates_data(engine, poste_demande):
    try:
        candidates_engine = create_engine(
            f"mysql+mysqlconnector://ai:XEVrQHF2PzXRE2UKmYKs@51.77.213.75:3307/boondmanager"
        )
        for table_name in ['CANDIDATES', 'candidates', 'Candidates']:
            try:
                with candidates_engine.connect() as connection:
                    connection.execute(text(f"SELECT 1 FROM `{table_name}` LIMIT 1")).fetchone()
                query = f"""
                    SELECT `firstName` AS `firstname`, 
                           `lastName` AS `lastname`, 
                           `title` AS `headline`,
                           `title` AS `poste`, 
                           `experiences` AS `description`, 
                           `availability` AS `experience`,
                           `diplomas` AS `skills`, 
                           `town` AS `city`, 
                           `socialNetworks` AS `urllinkedin`
                    FROM `{table_name}`
                    WHERE LOWER(`title`) LIKE LOWER(:poste)
                """
                with candidates_engine.connect() as connection:
                    df = pd.read_sql_query(text(query), connection, params={'poste': f'%{poste_demande}%'})
                if not df.empty:
                    df.columns = [col.lower() for col in df.columns]
                    print(f"Table {table_name} récupérée avec succès : {len(df)} lignes pour poste '{poste_demande}'")
                    return df
            except Exception:
                continue
        print(f"Aucune table CANDIDATES (ou variante) trouvée ou aucun candidat pour poste '{poste_demande}'.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erreur lors de la récupération de la table CANDIDATES: {e}\n{traceback.format_exc()}")
        return pd.DataFrame()

def format_demande_from_form_db(row_df):
    if row_df.empty:
        print("DataFrame vide pour format_demande_from_form_db.")
        return "N/A", None
    row = row_df.iloc[0]
    description = row.get('description', 'N/A')
    poste = row.get('poste', 'N/A')
    if description == 'N/A' or not description.strip():
        print(f"Avertissement: Description vide pour id {row.get('id', 'inconnu')}")
    demande_text = (
        f"Demande Client (Formulaire):\n"
        f"- Poste: {poste}\n"
        f"- Expérience: {row.get('experience_min', 0)} à {row.get('experience_max', 'N/A')} ans\n"
        f"- Compétences: {row.get('skills', 'N/A')}\n"
        f"- Environnement: {row.get('environment', 'N/A')}\n"
        f"- Langage: {row.get('language', 'N/A')}\n"
        f"- Domaine: {row.get('domain', 'N/A')}\n"
        f"- Ville(s): {row.get('ville', 'N/A')}\n"
        f"- Description: {description}"
    )
    return demande_text, poste

def format_demande_from_pdf_db(row_df):
    if row_df.empty:
        print("DataFrame vide pour format_demande_from_pdf_db.")
        return "N/A", None
    row = row_df.iloc[0]
    description = row.get('description', 'N/A')
    poste = row.get('poste', 'N/A')
    if description == 'N/A' or not description.strip():
        print(f"Avertissement: Description vide pour id {row.get('id', 'inconnu')}")
    demande_text = (
        f"Demande Client (PDF Extrait):\n"
        f"- Poste: {poste}\n"
        f"- Compétences: {row.get('skills', 'N/A')}\n"
        f"- Environnement: {row.get('environment', 'N/A')}\n"
        f"- Langage: {row.get('language', 'N/A')}\n"
        f"- Domaine: {row.get('domain', 'N/A')}\n"
        f"- Ville(s): {row.get('ville', 'N/A')}\n"
        f"- Description: {description}"
    )
    return demande_text, poste

def format_candidate_profile(row):
    firstname = row.get('firstname', 'N/A')
    lastname = row.get('lastname', 'N/A')
    poste = row.get('poste', 'N/A')
    headline = row.get('headline', 'N/A')
    experience = row.get('experience', 0)
    skills = row.get('skills', 'N/A')
    description = row.get('description', 'N/A')
    city = row.get('city', 'N/A')
    experience_str = f"{experience} an(s)" if pd.notna(experience) else "N/A"
    return (
        f"Nom : {firstname} {lastname}\nPoste : {poste}\nTitre : {headline}\n"
        f"Expérience : {experience_str}\nCompétences : {skills}\n"
        f"Description : {description}\nVille : {city}"
    )

def compute_similarity(text1, texts2_list, batch_size=100):
    if model_mini is None or model_distil is None:
        print("Erreur: Modèles de similarité non chargés.")
        return np.array([])
    if not isinstance(text1, str) or not text1.strip() or text1 == "N/A":
        print("Texte de référence (text1) pour la similarité est vide ou N/A.")
        return np.zeros(len(texts2_list)) if texts2_list else np.array([])
    if not texts2_list:
        print("La liste des profils candidats (texts2_list) est vide.")
        return np.array([])
    try:
        # Encoder le texte de la demande une seule fois
        emb_mini_1 = model_mini.encode(text1, convert_to_tensor=True, normalize_embeddings=True)
        emb_distil_1 = model_distil.encode(text1, convert_to_tensor=True, normalize_embeddings=True)

        scores = []
        for i in range(0, len(texts2_list), batch_size):
            batch_texts = texts2_list[i:i + batch_size]
            print(f"Traitement du lot {i // batch_size + 1} ({len(batch_texts)} profils)...")
            emb_mini_2 = model_mini.encode(batch_texts, convert_to_tensor=True, normalize_embeddings=True)
            emb_distil_2 = model_distil.encode(batch_texts, convert_to_tensor=True, normalize_embeddings=True)
            sim_mini = util.cos_sim(emb_mini_1, emb_mini_2).cpu().numpy()[0]
            sim_distil = util.cos_sim(emb_distil_1, emb_distil_2).cpu().numpy()[0]
            batch_scores = poids_mini * sim_mini + poids_distil * sim_distil
            scores.extend(batch_scores)
            del emb_mini_2, emb_distil_2  # Libérer la mémoire
        return np.array(scores)
    except Exception as e:
        print(f"Erreur dans compute_similarity: {e}\n{traceback.format_exc()}")
        return np.zeros(len(texts2_list))

def get_evaluation(score):
    if score >= 0.75:
        return "Parfait"
    elif score >= 0.40:
        return "Bon"
    elif score >= 0.15:
        return "Moyen"
    else:
        return "Faible"

def insert_into_profiles(results_df, engine):
    if results_df.empty:
        print("Aucun résultat à insérer dans PROFILES.")
        return
    try:
        with engine.connect() as connection:
            with connection.begin():
                inserted_count = 0
                for index, row in results_df.iterrows():
                    try:
                        # Tronquer les valeurs pour respecter les limites actuelles de PROFILES
                        firstname = str(row.get('firstname', 'N/A'))[:50]
                        lastname = str(row.get('lastname', 'N/A'))[:50]
                        poste = str(row.get('poste', 'N/A'))[:100]
                        linkedin_url = str(row.get('urllinkedin', ''))[:255]
                        score = float(row.get('score', 0.0))
                        score = min(max(score, -9.9999), 9.9999)  # Limite pour DECIMAL(5,4)
                        evaluation = str(row.get('evaluation', 'N/A'))[:50]

                        insert_query = text("""
                            INSERT INTO PROFILES (FIRSTNAME, LASTNAME, POSTE, LINKEDINURL, SCORE, EVALUATION)
                            VALUES (:fname, :lname, :poste, :url, :score, :eval)
                        """)
                        connection.execute(insert_query, {
                            "fname": firstname,
                            "lname": lastname,
                            "poste": poste,
                            "url": linkedin_url,
                            "score": score,
                            "eval": evaluation
                        })
                        inserted_count += 1
                    except Exception as e:
                        print(f"Erreur lors de l'insertion de la ligne {index} dans PROFILES: {e}")
                        print(f"Valeurs problématiques: {row.to_dict()}")
                        continue
                print(f"{inserted_count} résultats insérés dans la table PROFILES sur {len(results_df)}.")
    except Exception as e:
        print(f"Erreur générale lors de l'insertion dans PROFILES: {e}\n{traceback.format_exc()}")

def insert_workflow_status(engine, request_id, status):
    try:
        status_map = {
            "IN_PROGRESS": "IN_PROGRESS",
            "ERROR_NO_DEMANDE_DATA": "ERR_NO_DATA",
            "ERROR_NO_CANDIDATES": "ERR_NO_CAND",
            "ERROR_MISSING_CANDIDATE_COLS": "ERR_COLS",
            "ERROR_SIMILARITY_COMPUTATION": "ERR_SIM",
            "COMPLETED_NO_VALID_PROFILES": "NO_PROFILES",
            "COMPLETED": "COMPLETED",
            "ERROR_CRITICAL_MAIN_PROCESS": "ERR_CRIT",
            "ERROR_HANDLER_TRIGGER": "ERR_TRIG"
        }
        status_val = status_map.get(status, "UNKNOWN")
        with engine.connect() as connection:
            with connection.begin():
                upsert_query = text("""
                    INSERT INTO WORKFLOW_STATUS (REQUEST_ID, STATUS, LAST_UPDATED)
                    VALUES (:req_id, :status_val, NOW())
                    ON DUPLICATE KEY UPDATE
                        STATUS = :status_val,
                        LAST_UPDATED = NOW()
                """)
                connection.execute(upsert_query, {"req_id": request_id, "status_val": status_val})
                print(f"Workflow status '{status_val}' pour ID = {request_id}")
    except Exception as e:
        print(f"Erreur lors de l'insertion dans WORKFLOW_STATUS pour ID={request_id}: {e}\n{traceback.format_exc()}")

def main_process(engine, request_id):
    try:
        start_time = time.time()
        print(f"Début du traitement pour ID {request_id} à {time.strftime('%H:%M:%S')}")
        insert_workflow_status(engine, request_id, "IN_PROGRESS")

        # Déterminer la source de la demande
        print("Récupération des données de DEMANDES_TEXTES_NEW...")
        df_demande_pdf = fetch_latest_demande(engine, "DEMANDES_TEXTES_NEW", request_id)
        demande_client_text = None
        poste_demande = None
        effective_source_type = None

        if df_demande_pdf is not None and not df_demande_pdf.empty:
            demande_client_text, poste_demande = format_demande_from_pdf_db(df_demande_pdf)
            effective_source_type = "pdf"
            print(f"Utilisation de la source PDF pour id {request_id}")
        else:
            print("Récupération des données de DEMANDES...")
            df_demande_form = fetch_latest_demande(engine, "DEMANDES", request_id)
            if df_demande_form is not None and not df_demande_form.empty:
                demande_client_text, poste_demande = format_demande_from_form_db(df_demande_form)
                effective_source_type = "form"
                print(f"Utilisation de la source Formulaire pour id {request_id}")
            else:
                print(f"Aucune demande (PDF ou Formulaire) trouvée pour id {request_id}.")
                insert_workflow_status(engine, request_id, "ERROR_NO_DEMANDE_DATA")
                return {"status": "error", "message": "Aucune donnée de demande (PDF ou Formulaire) trouvée."}

        if not demande_client_text or demande_client_text == "N/A":
            print("Texte de la demande client vide ou N/A.")
            insert_workflow_status(engine, request_id, "ERROR_NO_DEMANDE_DATA")
            return {"status": "error", "message": "Texte de la demande client est vide ou N/A."}
        if not poste_demande or poste_demande == "N/A":
            print("Poste de la demande vide ou N/A.")
            insert_workflow_status(engine, request_id, "ERROR_NO_DEMANDE_DATA")
            return {"status": "error", "message": "Poste de la demande est vide ou N/A."}
        print(f"Demande client formatée ({effective_source_type}):\n{demande_client_text[:300]}...")

        # Récupérer les candidats avec le même poste
        print(f"Récupération des candidats pour poste '{poste_demande}'...")
        df_candidates = fetch_candidates_data(engine, poste_demande)
        if df_candidates.empty:
            print(f"Aucun candidat trouvé pour poste '{poste_demande}'.")
            insert_workflow_status(engine, request_id, "ERROR_NO_CANDIDATES")
            return {"status": "error", "message": f"Aucun candidat trouvé pour poste '{poste_demande}'"}

        required_cols_cand = ["firstname", "lastname", "headline", "poste", "description", "experience", "skills",
                              "city", "urllinkedin"]
        if not all(col in df_candidates.columns for col in required_cols_cand):
            missing_cols = [col for col in required_cols_cand if col not in df_candidates.columns]
            print(f"Colonnes manquantes dans CANDIDATES: {missing_cols}")
            insert_workflow_status(engine, request_id, "ERROR_MISSING_CANDIDATE_COLS")
            return {"status": "error", "message": f"Colonnes manquantes dans CANDIDATES: {missing_cols}"}

        df_candidates = df_candidates.drop_duplicates(subset=['firstname', 'lastname'], keep='first')
        print(f"Nb candidats uniques pour poste '{poste_demande}': {len(df_candidates)}")

        # Préparer profils et calculer similarité
        print("Formatage des profils candidats...")
        profile_texts_for_similarity = [format_candidate_profile(row) for _, row in df_candidates.iterrows()]
        valid_profiles_data = [{'original_index': df_candidates.index[i], 'text': pt} for i, pt in
                               enumerate(profile_texts_for_similarity) if pt and pt.strip()]
        if not valid_profiles_data:
            print("Aucun profil candidat valide pour similarité.")
            insert_workflow_status(engine, request_id, "COMPLETED_NO_VALID_PROFILES")
            return {"status": "success", "message": "Aucun profil candidat valide pour le matching.",
                    "processed_count": 0}

        valid_profile_texts_list = [item['text'] for item in valid_profiles_data]
        print(f"Calcul de la similarité pour {len(valid_profile_texts_list)} profils...")
        scores = compute_similarity(demande_client_text, valid_profile_texts_list)
        if scores is None or len(scores) != len(valid_profile_texts_list):
            print("Erreur calcul scores ou taille incohérente.")
            insert_workflow_status(engine, request_id, "ERROR_SIMILARITY_COMPUTATION")
            return {"status": "error", "message": "Erreur de calcul de similarité"}

        # Construire et insérer résultats
        results_list = []
        for i, score_value in enumerate(scores):
            original_idx = valid_profiles_data[i]['original_index']
            candidate_row = df_candidates.loc[original_idx]
            results_list.append({
                "firstname": candidate_row.get('firstname', 'N/A'),
                "lastname": candidate_row.get('lastname', 'N/A'),
                "headline": candidate_row.get('headline', 'N/A'),
                "poste": candidate_row.get('poste', 'N/A'),
                "description": candidate_row.get('description', 'N/A'),
                "experience": candidate_row.get('experience', 'N/A'),
                "skills": candidate_row.get('skills', 'N/A'),
                "city": candidate_row.get('city', 'N/A'),
                "urllinkedin": candidate_row.get('urllinkedin', ''),
                "score": round(float(score_value), 4),
                "evaluation": get_evaluation(score_value)
            })
        results_df = pd.DataFrame(results_list)
        if not results_df.empty:
            results_df = results_df.sort_values(by="score", ascending=False)
            print("Insertion des résultats dans PROFILES...")
            insert_into_profiles(results_df, engine)
        else:
            print("Aucun résultat de matching généré après calcul des scores.")

        insert_workflow_status(engine, request_id, "COMPLETED")
        end_time = time.time()
        print(f"Terminé pour id {request_id}. Temps: {end_time - start_time:.2f}s. {len(results_df)} profils traités.")
        return {"status": "success", "message": f"Traitement terminé. {len(results_df)} profils traités.",
                "processed_count": len(results_df)}

    except Exception as e:
        print(f"Erreur majeure dans main_process pour id {request_id}: {e}\n{traceback.format_exc()}")
        try:
            insert_workflow_status(engine, request_id, "ERROR_CRITICAL_MAIN_PROCESS")
        except Exception as e_status:
            print(f"Impossible de MAJ statut erreur critique pour {request_id}: {e_status}\n{traceback.format_exc()}")
        return {"status": "error", "message": f"Erreur critique: {str(e)}"}

@app.route('/trigger', methods=['POST', 'GET'])
def trigger():
    request_id = request.args.get('request_id')
    if request.is_json:
        data = request.get_json()
        request_id = data.get('request_id', request_id)
        if data.get('test') == "ok":
            request_id = request_id or str(uuid.uuid4())

    print(f"Déclenchement reçu. ID: {request_id}")

    if not request_id:
        error_message = "Erreur: ID manquant. Fournissez un 'request_id' dans les paramètres de l'URL ou le corps JSON."
        print(error_message)
        return jsonify({"status": "error", "message": error_message}), 400

    if model_mini is None or model_distil is None:
        print("Erreur: Modèles SentenceTransformer non chargés.")
        return jsonify({"status": "error", "message": "Erreur serveur: modèles de similarité non disponibles."}), 500

    engine = None
    try:
        engine = get_snowflake_engine()
        result = main_process(engine, request_id)
        return jsonify(result)
    except Exception as e:
        print(f"Erreur dans le handler /trigger: {e}\n{traceback.format_exc()}")
        if engine and request_id:
            try:
                insert_workflow_status(engine, request_id, "ERROR_HANDLER_TRIGGER")
            except Exception as e_status:
                print(
                    f"Impossible de MAJ statut erreur (handler) pour {request_id}: {e_status}\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": f"Erreur serveur interne: {str(e)}"}), 500
    finally:
        if engine:
            engine.dispose()

if __name__ == '__main__':
    print("Démarrage du serveur Flask de développement sur le port 5050...")
    app.run(host='0.0.0.0', port=5050, debug=False)
