import streamlit as st
import mysql.connector
import pandas as pd
import requests
import time
import uuid
from pypdf import PdfReader
from datetime import datetime
import json
import re

# Dictionnaire de mappage ville -> région 
VILLE_TO_REGION = {
    # Auvergne-Rhône-Alpes
    'Lyon': 'Auvergne-Rhône-Alpes',
    'Grenoble': 'Auvergne-Rhône-Alpes',
    'Saint-Étienne': 'Auvergne-Rhône-Alpes',
    'Clermont-Ferrand': 'Auvergne-Rhône-Alpes',
    'Annecy': 'Auvergne-Rhône-Alpes',
    'Chambéry': 'Auvergne-Rhône-Alpes',
    'Valence': 'Auvergne-Rhône-Alpes',
    'Bourg-en-Bresse': 'Auvergne-Rhône-Alpes',
    'Aurillac': 'Auvergne-Rhône-Alpes',
    'Vienne': 'Auvergne-Rhône-Alpes',

    # Bourgogne-Franche-Comté
    'Dijon': 'Bourgogne-Franche-Comté',
    'Besançon': 'Bourgogne-Franche-Comté',
    'Belfort': 'Bourgogne-Franche-Comté',
    'Chalon-sur-Saône': 'Bourgogne-Franche-Comté',
    'Mâcon': 'Bourgogne-Franche-Comté',
    'Nevers': 'Bourgogne-Franche-Comté',
    'Auxerre': 'Bourgogne-Franche-Comté',
    'Lons-le-Saunier': 'Bourgogne-Franche-Comté',

    # Bretagne
    'Rennes': 'Bretagne',
    'Brest': 'Bretagne',
    'Quimper': 'Bretagne',
    'Lorient': 'Bretagne',
    'Vannes': 'Bretagne',
    'Saint-Malo': 'Bretagne',
    'Saint-Brieuc': 'Bretagne',

    # Centre-Val de Loire
    'Orléans': 'Centre-Val de Loire',
    'Tours': 'Centre-Val de Loire',
    'Blois': 'Centre-Val de Loire',
    'Bourges': 'Centre-Val de Loire',
    'Chartres': 'Centre-Val de Loire',
    'Châteauroux': 'Centre-Val de Loire',

    # Corse
    'Ajaccio': 'Corse',
    'Bastia': 'Corse',
    'Corte': 'Corse',
    'Porto-Vecchio': 'Corse',

    # Grand Est
    'Strasbourg': 'Grand Est',
    'Nancy': 'Grand Est',
    'Metz': 'Grand Est',
    'Reims': 'Grand Est',
    'Mulhouse': 'Grand Est',
    'Troyes': 'Grand Est',
    'Colmar': 'Grand Est',
    'Épinal': 'Grand Est',
    'Châlons-en-Champagne': 'Grand Est',

    # Hauts-de-France
    'Lille': 'Hauts-de-France',
    'Amiens': 'Hauts-de-France',
    'Dunkerque': 'Hauts-de-France',
    'Calais': 'Hauts-de-France',
    'Arras': 'Hauts-de-France',
    'Boulogne-sur-Mer': 'Hauts-de-France',
    'Saint-Quentin': 'Hauts-de-France',
    'Beauvais': 'Hauts-de-France',

    # Île-de-France
    'Paris': 'Île-de-France',
    'Versailles': 'Île-de-France',
    'Boulogne-Billancourt': 'Île-de-France',
    'Saint-Denis': 'Île-de-France',
    'Nanterre': 'Île-de-France',
    'Créteil': 'Île-de-France',
    'Évry': 'Île-de-France',
    'Cergy': 'Île-de-France',

    # Normandie
    'Rouen': 'Normandie',
    'Caen': 'Normandie',
    'Le Havre': 'Normandie',
    'Cherbourg-en-Cotentin': 'Normandie',
    'Évreux': 'Normandie',
    'Alençon': 'Normandie',

    # Nouvelle-Aquitaine
    'Bordeaux': 'Nouvelle-Aquitaine',
    'Poitiers': 'Nouvelle-Aquitaine',
    'Limoges': 'Nouvelle-Aquitaine',
    'La Rochelle': 'Nouvelle-Aquitaine',
    'Pau': 'Nouvelle-Aquitaine',
    'Bayonne': 'Nouvelle-Aquitaine',
    'Angoulême': 'Nouvelle-Aquitaine',
    'Niort': 'Nouvelle-Aquitaine',
    'Périgueux': 'Nouvelle-Aquitaine',

    # Occitanie
    'Toulouse': 'Occitanie',
    'Montpellier': 'Occitanie',
    'Nîmes': 'Occitanie',
    'Perpignan': 'Occitanie',
    'Albi': 'Occitanie',
    'Carcassonne': 'Occitanie',
    'Montauban': 'Occitanie',
    'Tarbes': 'Occitanie',

    # Pays de la Loire
    'Nantes': 'Pays de la Loire',
    'Angers': 'Pays de la Loire',
    'Le Mans': 'Pays de la Loire',
    'Saint-Nazaire': 'Pays de la Loire',
    'La Roche-sur-Yon': 'Pays de la Loire',
    'Cholet': 'Pays de la Loire',

    # Provence-Alpes-Côte d’Azur
    'Marseille': 'Provence-Alpes-Côte d’Azur',
    'Nice': 'Provence-Alpes-Côte d’Azur',
    'Toulon': 'Provence-Alpes-Côte d’Azur',
    'Aix-en-Provence': 'Provence-Alpes-Côte d’Azur',
    'Avignon': 'Provence-Alpes-Côte d’Azur',
    'Cannes': 'Provence-Alpes-Côte d’Azur',
    'Antibes': 'Provence-Alpes-Côte d’Azur',
    'Gap': 'Provence-Alpes-Côte d’Azur',

    # Régions d'outre-mer
    'Basse-Terre': 'Guadeloupe',
    'Pointe-à-Pitre': 'Guadeloupe',
    'Fort-de-France': 'Martinique',
    'Le Lamentin': 'Martinique',
    'Cayenne': 'Guyane',
    'Saint-Laurent-du-Maroni': 'Guyane',
    'Saint-Denis': 'La Réunion',
    'Saint-Pierre': 'La Réunion',
    'Mamoudzou': 'Mayotte'
}

# CSS styling
st.markdown("""
   <style>
    .main { background-color: #f5f7fa; }
    .stButton>button {
        background-color: #1a3c6d;
        color: white;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #2a5c9d;
    }
    .stTextInput>div>input,
    .stNumberInput>div>input,
    .stTextArea>div>textarea,
    .stSelectbox select {
        border-radius: 8px;
        border: 1px solid #ccc;
    }
    .header {
        text-align: center;
        color: #1a3c6d;
        font-size: 2.5em;
        margin-bottom: 10px;
    }
    .subheader {
        color: #2a5c9d;
        font-size: 1.5em;
        margin-top: 20px;
    }
    .content {
        padding: 20px;
        text-align: left;
    }
    .stDataFrame {
        border: 1px solid #ddd;
        border-radius: 8px;
        font-size: 18px;
        width: 100% !important;
        max-width: 1400px;
        margin: 0 auto;
    }
    .stDataFrame table {
        width: 100% !important;
        border-collapse: collapse;
    }
    .stDataFrame th,
    .stDataFrame td {
        padding: 15px;
        font-size: 18px;
        line-height: 1.6;
    }
    .status-box {
        padding: 10px;
        border-radius: 8px;
        background-color: #e6f0fa;
    }
    .footer {
        text-align: center;
        margin-top: 20px;
        color: #666;
    }
    .home-header {
        text-align: center;
        color: #1a3c6d;
        font-size: 2.2em;
        margin-bottom: 15px;
        font-weight: 700;
    }
    .home-subheader {
        text-align: center;
        color: #2a5c9d;
        font-size: 1.5em;
        margin-top: 5px;
        margin-bottom: 30px;
        font-weight: 500;
    }
    .home-column-content {
        padding: 20px;
        margin-bottom: 20px;
        text-align: center;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .home-column-content strong {
        color: #1a3c6d;
        font-size: 1.1em;
        margin-top: 10px;
        margin-bottom: 10px;
        display: block;
    }
    .home-column-content p {
        font-size: 0.9em;
        color: #555;
        flex-grow: 1;
        margin-bottom: 15px;
    }
    .home-column-content .stButton>button {
        width: 100%;
        padding: 10px 0;
        font-size: 0.95em;
        margin-top: auto;
    }
    .home-column-content img {
        display: block;
        margin: 150px auto;
        width: 35px;
        height: 35px;
    }
    div[data-testid="stHorizontalBlock"] > div[data-testid^="stColumn"] {
        display: flex;
        flex-direction: column;
        height: 100%;
        justify-content: flex-start;
    }
    .stImage {
        text-align: center;
    }
    .stImage > img {
        display: block;
        margin: 0 auto 20px;
        max-width: 130px;
    }
    @media (max-width: 768px) {
        div[data-testid="stButton"] > button:not(.home-column-content button):not(.back-button-custom-style) {
            display: block;
            margin-left: auto !important;
            margin-right: auto !important;
            width: 80%;
            max-width: 300px;
            margin-top: 0.75rem;
            margin-bottom: 0.75rem;
        }
        div[data-testid="stMarkdownContainer"]:not(.home-column-content div[data-testid="stMarkdownContainer"]),
        div[data-testid="stText"],
        div[data-testid="stHeader"]:not(.header):not(.home-header),
        div[data-testid="stSubheader"]:not(.subheader):not(.home-subheader),
        div[data-testid="stCaption"],
        div[data-testid="stMetric"] > div[data-testid="stMetricValue"],
        div[data-testid="stMetric"] > div[data-testid="stMetricLabel"],
        h1[data-testid="stHeading"]:not(.header):not(.home-header) {
            text-align: center;
            padding-left: 5px;
            padding-right: 5px;
        }
        .header, h1[data-testid="stHeading"] { font-size: 2em; }
        .subheader { font-size: 1.3em; }
        .home-header { font-size: 1.8em; }
        .home-subheader { font-size: 1.2em; }
        .stDataFrame,
        .stDataFrame th,
        .stDataFrame td {
            font-size: 14px;
            padding: 8px;
        }
    }
   </style>
""", unsafe_allow_html=True)

# Initialize MySQL connections
try:
    mysql_interfaces_conn = mysql.connector.connect(
        host="51.77.213.75",
        port=3307,
        user="ai",
        password="XEVrQHF2PzXRE2UKmYKs",
        database="interfaces"
    )
except Exception as e:
    st.error(f"Erreur de connexion à MySQL (interfaces) : {e}")
    st.stop()

try:
    mysql_boondmanager_conn = mysql.connector.connect(
        host="51.77.213.75",
        port=3307,
        user="ai",
        password="XEVrQHF2PzXRE2UKmYKs",
        database="boondmanager"
    )
except Exception as e:
    st.error(f"Erreur de connexion à MySQL (boondmanager) : {e}")
    st.stop()

# Initialize session state
if 'page' not in st.session_state:
    st.session_state.page = 'home'
if 'submit_triggered' not in st.session_state:
    st.session_state.submit_triggered = False
if 'results_ready' not in st.session_state:
    st.session_state.results_ready = False
if 'workflow_completed' not in st.session_state:
    st.session_state.workflow_completed = False
if 'request_id' not in st.session_state:
    st.session_state.request_id = None

# Function to clear tables (MySQL)
def clear_tables(tables_to_clear):
    cursor_clear = None
    try:
        cursor_clear = mysql_interfaces_conn.cursor()
        for table in tables_to_clear:
            cursor_clear.execute(f"TRUNCATE TABLE {table}")
        mysql_interfaces_conn.commit()

    except Exception as e:
        st.error(f"Erreur lors du vidage des tables : {e}")
    finally:
        if cursor_clear:
            cursor_clear.close()

# Function to verify table contents with SELECT *
def verify_tables():
    cursor_verify = None
    try:
        cursor_verify = mysql_interfaces_conn.cursor()
        cursor_verify.execute("SHOW TABLES")
        tables = cursor_verify.fetchall()
        
        st.markdown("### Vérification des tables dans la base 'interfaces'")
        for table in tables:
            table_name = table[0]
            st.markdown(f"#### Table : {table_name}")
            try:
                cursor_verify.execute(f"SELECT * FROM {table_name}")
                rows = cursor_verify.fetchall()
                columns = [desc[0] for desc in cursor_verify.description]
                st.write("**Colonnes** :", columns)
                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df, use_container_width=True)
                else:
                    st.info(f"Aucune donnée dans la table {table_name}.")
            except Exception as e:
                st.error(f"Erreur lors de l'interrogation de la table {table_name} : {e}")
    except Exception as e:
        st.error(f"Erreur lors de la récupération des tables : {e}")
    finally:
        if cursor_verify:
            cursor_verify.close()

# Function to send data to webhook with enhanced debugging
def send_to_webhook(webhook_url, payload):
    try:
        response = requests.post(webhook_url, json=payload, timeout=60)
        if response.ok:
            st.success("Requête envoyée à n8n avec succès !")
            return True
        else:
            st.error(f"Erreur lors de l'envoi à n8n : {response.status_code} - {response.text}")
            return False
    except requests.exceptions.Timeout:
        st.error("Erreur lors de la requête à n8n : Timeout (délai dépassé après 60 secondes).")
        return False
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur lors de la requête à n8n : {str(e)}")
        return False

# Function to check workflow status in MySQL
def check_workflow_status_in_mysql(request_id):
    cursor_status = None
    try:
        cursor_status = mysql_interfaces_conn.cursor()
        query = """
            SELECT STATUS
            FROM WORKFLOW_STATUS
            WHERE id = %s
            LIMIT 1
        """
        cursor_status.execute(query, (request_id,))
        result = cursor_status.fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        return None
    finally:
        if cursor_status:
            cursor_status.close()

# Function to display profiles data
def display_profiles_data():
    cursor_tmp = None
    try:
        cursor_tmp = mysql_interfaces_conn.cursor()
        query = """
            SELECT
                p.FIRSTNAME,
                p.LASTNAME,
                p.SCORE,
                p.EVALUATION,
                p.LINKEDINURL AS URL_LINKEDIN,
                p.POSTE,
                c.town AS VILLE,
                c.email1 AS EMAIL,
                c.experiences AS EXPERIENCE
            FROM interfaces.PROFILES p
            LEFT JOIN boondmanager.candidates c
                ON UPPER(TRIM(COALESCE(p.FIRSTNAME, ''))) = UPPER(TRIM(COALESCE(c.firstName, '')))
                AND UPPER(TRIM(COALESCE(p.LASTNAME, ''))) = UPPER(TRIM(COALESCE(c.lastName, '')))
        """
        cursor_tmp.execute(query)
        profiles_data = cursor_tmp.fetchall()
        columns = [desc[0] for desc in cursor_tmp.description]
        profiles_df = pd.DataFrame(profiles_data, columns=columns)

        # Fonction pour parser la colonne URL_LINKEDIN
        def parse_linkedin_url(linkedin_data):
            if pd.isna(linkedin_data) or not linkedin_data:
                return None
            try:
                # Parser la chaîne JSON
                networks = json.loads(linkedin_data)
                # Rechercher l'entrée LinkedIn
                for network in networks:
                    if network.get('network') == 'linkedin' and network.get('url'):
                        return network['url']
                return None
            except json.JSONDecodeError:
                # Si la chaîne n'est pas un JSON valide, supposer qu'elle est déjà une URL
                if linkedin_data.startswith('https://www.linkedin.com'):
                    return linkedin_data
                return None

        # Appliquer le parsing à la colonne URL_LINKEDIN
        profiles_df['linkedin_url'] = profiles_df['URL_LINKEDIN'].apply(parse_linkedin_url)

        if not profiles_df.empty:
            st.write("---")
            st.markdown("#### Filtrer les résultats")

            # Ajouter la colonne REGION en mappant les villes
            profiles_df['REGION'] = profiles_df['VILLE'].map(VILLE_TO_REGION).fillna('Région inconnue')

            # Disposition des filtres en colonnes
            col_filters_1, col_filters_2, col_filters_3 = st.columns(3)

            with col_filters_1:
                min_score_val = 0.0 if profiles_df['SCORE'].empty or profiles_df['SCORE'].isna().all() else float(profiles_df['SCORE'].min())
                max_score_val = 1.0 if profiles_df['SCORE'].empty or profiles_df['SCORE'].isna().all() else float(profiles_df['SCORE'].max())
                if min_score_val > max_score_val:
                    max_score_val = min_score_val
                if min_score_val == max_score_val and min_score_val == 0.0:
                    max_score_val = 1.0

                score_range = st.slider(
                    "Filtrer par Score",
                    min_value=min_score_val,
                    max_value=max_score_val,
                    value=(min_score_val, max_score_val),
                    step=0.01,
                    format="%.2f",
                    key="slider_score_profiles"
                )

            with col_filters_2:
                if 'REGION' in profiles_df.columns and not profiles_df['REGION'].isna().all():
                    unique_regions = sorted(profiles_df['REGION'].dropna().unique().tolist())
                    all_regions = ['Toutes'] + unique_regions
                    selected_regions_filter = st.multiselect("Région", all_regions, default=['Toutes'], key="multiselect_region_profiles")
                else:
                    selected_regions_filter = ['Toutes']

            with col_filters_3:
                if 'VILLE' in profiles_df.columns and not profiles_df['VILLE'].isna().all():
                    unique_villes = sorted(profiles_df['VILLE'].dropna().unique().tolist())
                    all_villes = ['Toutes'] + unique_villes
                    selected_villes_filter = st.multiselect("Filtrer par Ville", all_villes, default=['Toutes'], key="multiselect_ville_profiles")
                else:
                    selected_villes_filter = ['Toutes']

            # Appliquer les filtres
            filtered_df = profiles_df.copy()
            if not filtered_df['SCORE'].empty:
                filtered_df = filtered_df[
                    (filtered_df['SCORE'] >= score_range[0]) &
                    (filtered_df['SCORE'] <= score_range[1])
                ]

            if 'Toutes' not in selected_regions_filter and 'REGION' in filtered_df.columns and not filtered_df['REGION'].isna().all():
                filtered_df = filtered_df[filtered_df['REGION'].isin(selected_regions_filter)]

            if 'Toutes' not in selected_villes_filter and 'VILLE' in filtered_df.columns and not filtered_df['VILLE'].isna().all():
                filtered_df = filtered_df[filtered_df['VILLE'].isin(selected_villes_filter)]

            st.write("---")

            display_df_for_table = filtered_df.copy()
            if 'POSTE' in display_df_for_table.columns:
                display_df_for_table.rename(columns={'POSTE': 'POSTE_MATCHE'}, inplace=True)

            st.dataframe(
                display_df_for_table,
                use_container_width=True,
                column_config={
                    "FIRSTNAME": "Prénom",
                    "LASTNAME": "Nom",
                    "SCORE": st.column_config.NumberColumn("Score", format="%.2f"),
                    "EVALUATION": st.column_config.TextColumn("Évaluation (Résumé)", width="large"),
                    "linkedin_url": st.column_config.LinkColumn("Profil LinkedIn", display_text="Voir Profil", width="small"),
                    "POSTE_MATCHE": st.column_config.TextColumn("Poste Matché"),
                    "VILLE": "Ville",
                    "EMAIL": st.column_config.TextColumn("Email", width="medium"),
                },
                column_order=["FIRSTNAME", "LASTNAME", "SCORE", "EVALUATION", "linkedin_url", "POSTE_MATCHE", "VILLE", "EMAIL"],
                key="profiles_dataframe_display"
            )

            if not filtered_df.empty:
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                st.download_button("Télécharger les résultats filtrés (CSV)", csv, "profiles_filtered.csv", "text/csv", key="download_profiles_csv")
                st.session_state.results_ready = True

                st.write("---")
                st.markdown("#### Détails du Profil Sélectionné et Évaluation Complète")

                profile_options_detail = [
                    f"{idx}: {row['FIRSTNAME']} {row['LASTNAME']} ({row['POSTE']})"
                    for idx, row in filtered_df.iterrows()
                    if pd.notna(row['POSTE']) and pd.notna(row['FIRSTNAME']) and pd.notna(row['LASTNAME'])
                ]

                if not profile_options_detail:
                    st.info("Aucun profil avec un poste défini à afficher pour les détails après filtrage.")
                else:
                    selected_profile_display_with_idx = st.selectbox(
                        "Sélectionnez un profil pour voir ses détails",
                        profile_options_detail,
                        index=0,
                        key="select_profile_detail_dropdown"
                    )

                    if selected_profile_display_with_idx:
                        selected_idx = int(selected_profile_display_with_idx.split(":")[0])
                        selected_profile_row = filtered_df.loc[selected_idx]

                        st.markdown(f"**Profil de :** {selected_profile_row['FIRSTNAME']} {selected_profile_row['LASTNAME']}")
                        st.markdown(f"**Poste (tel que matché) :** {selected_profile_row['POSTE']}")
                        st.markdown(f"**Score de matching :** {selected_profile_row['SCORE']:.2f}")

                        # Affichage structuré des expériences
                        if 'EXPERIENCE' in selected_profile_row and pd.notna(selected_profile_row['EXPERIENCE']):
                            try:
                                # Parser la chaîne JSON
                                experiences = json.loads(selected_profile_row['EXPERIENCE'])
                                if not isinstance(experiences, list):
                                    experiences = [experiences]  # Convertir en liste si ce n'est pas déjà le cas
                                st.markdown("**Expériences :**")
                                
                                # Style CSS pour les cartes d'expérience
                                st.markdown("""
                                    <style>
                                        .experience-card {
                                            background-color: #f9fafc;
                                            border: 1px solid #e0e4e8;
                                            border-radius: 8px;
                                            padding: 15px;
                                            margin-bottom: 10px;
                                            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                                        }
                                        .experience-card h4 {
                                            margin: 0 0 10px 0;
                                            color: #1a3c6d;
                                            font-size: 1.1em;
                                        }
                                        .experience-card p {
                                            margin: 5px 0;
                                            color: #333;
                                            font-size: 0.95em;
                                        }
                                        .experience-card .label {
                                            font-weight: bold;
                                            color: #2a5c9d;
                                        }
                                    </style>
                                """, unsafe_allow_html=True)

                                # Fonction pour extraire les dates du champ title
                                def extract_dates_from_title(title):
                                    date_pattern = r'(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})'
                                    match = re.search(date_pattern, title)
                                    if match:
                                        start_date, end_date = match.groups()
                                        cleaned_title = re.sub(date_pattern, '', title).strip()
                                        return start_date, end_date, cleaned_title
                                    return None, None, title

                                # Afficher chaque expérience sous forme de carte
                                for exp in experiences:
                                    title = exp.get('title', 'Titre non spécifié')
                                    description = exp.get('description', 'Aucune description')
                                    company = exp.get('company', 'Non spécifié')
                                    location = exp.get('location', 'Non spécifié')
                                    skills = exp.get('skills', 'Non spécifié')

                                    # Extraire les dates du title si disponibles
                                    start_date, end_date, cleaned_title = extract_dates_from_title(title)
                                    if not start_date:
                                        start_date = exp.get('startDate', exp.get('startYear', 'Non spécifié'))
                                        end_date = exp.get('endDate', exp.get('endYear', 'Non spécifié'))

                                    # Construire les dates
                                    date_range = f"{start_date} - {end_date}" if start_date != 'Non spécifié' and end_date != 'Non spécifié' else "Dates non spécifiées"

                                    # Afficher la carte d'expérience
                                    st.markdown(f"""
                                        <div class="experience-card">
                                            <h4>{cleaned_title}</h4>
                                            <p><span class="label">Description :</span> {description}</p>
                                            <p><span class="label">Entreprise :</span> {company}</p>
                                            <p><span class="label">Lieu :</span> {location}</p>
                                            <p><span class="label">Période :</span> {date_range}</p>
                                            <p><span class="label">Compétences :</span> {skills}</p>
                                        </div>
                                    """, unsafe_allow_html=True)
                            except json.JSONDecodeError:
                                st.warning("Erreur : Le format des expériences n'est pas valide (JSON incorrect). Affichage brut :")
                                st.markdown(f"{selected_profile_row['EXPERIENCE']}")
                            except Exception as e:
                                st.warning(f"Erreur lors du traitement des expériences : {str(e)}. Affichage brut :")
                                st.markdown(f"{selected_profile_row['EXPERIENCE']}")
                        else:
                            st.markdown("**Expériences :** Non disponibles")

                        # Affichage des autres champs
                        if 'VILLE' in selected_profile_row and pd.notna(selected_profile_row['VILLE']):
                            st.markdown(f"**Ville :** {selected_profile_row['VILLE']}")
                        if 'PAYS' in selected_profile_row and pd.notna(selected_profile_row['PAYS']):
                            st.markdown(f"**Pays :** {selected_profile_row['PAYS']}")
                        if 'linkedin_url' in selected_profile_row and pd.notna(selected_profile_row['linkedin_url']):
                            st.markdown(f"**LinkedIn :** [Voir le profil]({selected_profile_row['linkedin_url']})")
                        if 'EMAIL' in selected_profile_row and pd.notna(selected_profile_row['EMAIL']):
                            st.markdown(f"**Email :** {selected_profile_row['EMAIL']}")

                        st.markdown("---")
                        st.markdown(f"**Évaluation Complète du Matching :**")
                        st.markdown(f"<div style='background-color: #f0f2f6; border-left: 5px solid #1a3c6d; padding: 15px; border-radius: 5px; font-family: sans-serif; font-size: 1rem; line-height: 1.6;'>{selected_profile_row['EVALUATION']}</div>", unsafe_allow_html=True)
            else:
                st.info("Aucun profil ne correspond à vos filtres actuels.")
        else:
            st.info("Aucun profil n'a été trouvé ou traité pour le moment.")
    except Exception as e:
        st.error(f"Erreur inattendue lors de l'affichage des profils : {e}")
    finally:
        if cursor_tmp:
            cursor_tmp.close()

# Function to poll and display profiles
def poll_and_display_profiles():
    if st.session_state.request_id is None:
        st.error("Erreur : Aucun request_id disponible pour vérifier le statut du workflow.")
        return
    
    request_id = st.session_state.request_id
    status_placeholder = st.empty()
    
    with status_placeholder.container():
        st.info("🔄 Vérification du statut du traitement en cours...")
        progress_bar_n8n = st.progress(0)
    
    start_time_n8n = time.time()
    max_duration_n8n = 10

    workflow_completed = False
    for i in range(int(max_duration_n8n / 2.5)):
        time.sleep(2.5)
        elapsed_n8n = time.time() - start_time_n8n
        
        with status_placeholder.container():
            st.info(f"⏳ Traitement en cours... ({int(elapsed_n8n)}s / {max_duration_n8n}s)")
            progress_bar_n8n.progress(min(elapsed_n8n / max_duration_n8n, 1.0))

        n8n_status = check_workflow_status_in_mysql(request_id)
        if n8n_status == "COMPLETED":
            with status_placeholder.container():
                st.success("✅ C'est parfait ! Traitement terminé. Affichage des profils...")
            workflow_completed = True
            break

    if workflow_completed:
        display_profiles_data()
        st.session_state.workflow_completed = True
    else:
        with status_placeholder.container():
                st.success("✅ C'est parfait ! Traitement terminé. Affichage des profils...")
        
        time.sleep(1)
        
        with status_placeholder.container():
            st.info("🔄 Recherche de profils dans la base de données...")
            progress_bar_profiles = st.progress(0)
        
        start_time_profiles = time.time()
        max_duration_profiles = 120

        profiles_found = False
        for i in range(int(max_duration_profiles / 2.5)):
            time.sleep(2.5)
            elapsed_profiles = time.time() - start_time_profiles
            with status_placeholder.container():
                st.info(f"⏳ Recherche de profils... ({int(elapsed_profiles)}s / {max_duration_profiles}s)")
                progress_bar_profiles.progress(min(elapsed_profiles / max_duration_profiles, 1.0))

            cursor_count = None
            try:
                cursor_count = mysql_interfaces_conn.cursor()
                cursor_count.execute("SELECT COUNT(*) FROM PROFILES")
                count = cursor_count.fetchone()[0]
                if count > 0:
                    profiles_found = True
                    break
            except Exception as e:
                with status_placeholder.container():
                    st.error(f"Erreur lors de la vérification des profils : {e}")
                return
            finally:
                if cursor_count:
                    cursor_count.close()
        
        with status_placeholder.container():
            if profiles_found:
                st.success("✅ C'est parfait ! Profils trouvés. Affichage des profils...")
                display_profiles_data()
                st.session_state.workflow_completed = True
            else:
                st.error("❌ Erreur : Aucun profil trouvé et statut 'COMPLETED' non détecté dans WORKFLOW_STATUS.")

# Function to display all candidates
def display_all_candidates():
    st.markdown("""
        <div class="custom-back-button-container">
            <style>
                .custom-back-button-container {
                    position: absolute;
                    top: 15px;
                    right: 15px;
                    z-index: 999;
                }
                .back-button-custom-style {
                    background-color: #DD6436 !important;
                    color: white !important;
                    border: none;
                    padding: 8px 16px;
                    border-radius: 8px;
                    cursor: pointer;
                    font-weight: bold;
                }
                .back-button-custom-style:hover {
                    background-color: #c95427 !important;
                }
            </style>
        </div>
    """, unsafe_allow_html=True)
  
    cols_header = st.columns([0.8, 0.2])
    with cols_header[1]:
        if st.button("Accueil", key="back_button_all_candidates_page", help="Retourner à la page d'accueil"):
            st.session_state.page = 'home'
            st.rerun()

    st.markdown(f'<div class="header" style="margin-top: 0px; padding-top:0px;">Assistant Wevii</div>', unsafe_allow_html=True)
    st.markdown('<div class="subheader">Candidats Enregistrés</div>', unsafe_allow_html=True)

    cursor_all_candidates = None
    try:
        cursor_all_candidates = mysql_boondmanager_conn.cursor()
        cursor_all_candidates.execute("""
            SELECT
                id, firstName, lastName, title, email1, email2, email3, phone1, phone2,
                town, country, skills, diplomas, experiences, availability,
                globalEvaluation, creationDate, updateDate, isVisible, thumbnail,
                sourceDetail, socialNetworks
            FROM candidates
            ORDER BY lastName, firstName
        """)
        all_candidates_data = cursor_all_candidates.fetchall()
        all_candidates_df = pd.DataFrame(
            all_candidates_data,
            columns=[
                'id', 'firstName', 'lastName', 'title', 'email1', 'email2', 'email3',
                'phone1', 'phone2', 'town', 'country', 'skills', 'diplomas',
                'experiences', 'availability', 'globalEvaluation', 'creationDate',
                'updateDate', 'isVisible', 'thumbnail', 'sourceDetail', 'socialNetworks'
            ]
        )

        if not all_candidates_df.empty:
            st.write("---")
            st.markdown("#### Options d'Affichage et de Filtrage")

            col_f1, col_f2 = st.columns([1, 1, 1])

            with col_f1:
                max_r = len(all_candidates_df)
                def_r = min(50, max_r) if max_r > 0 else 0
                num_r = st.slider("Nombre à afficher", 0, max_r, def_r, 10, key="slider_all_cand_rows", help="Ajustez le nombre de candidats à visualiser.")

            df_to_filter = all_candidates_df.copy()

            
            with col_f2:
                if 'town' in df_to_filter.columns and not df_to_filter['town'].isna().all():
                    unique_v = sorted(df_to_filter['town'].dropna().unique().tolist())
                    sel_v = st.multiselect("Filtrer par Ville", ['Toutes'] + unique_v, default=['Toutes'], key="ms_all_cand_town")
                    if 'Toutes' not in sel_v:
                        df_to_filter = df_to_filter[df_to_filter['town'].isin(sel_v)]
            
            final_df_to_display = df_to_filter.head(num_r)

            st.write("---")
            if not final_df_to_display.empty:
                st.dataframe(
                    final_df_to_display,
                    use_container_width=True,
                    column_config={
                        "firstName": "Prénom",
                        "lastName": "Nom",
                        "title": "Poste/Titre",
                        "town": "Ville",
                        "country": "Pays",
                        "skills": st.column_config.TextColumn("Compétences", width="medium"),
                        "experiences": st.column_config.TextColumn("Expérience", width="small"),
                        "socialNetworks": st.column_config.LinkColumn("LinkedIn", display_text="Voir Profil", width="small"),
                        "email1": st.column_config.TextColumn("Email Principal", width="medium"),
                        "phone1": st.column_config.TextColumn("Téléphone Principal", width="small")
                    },
                    key="df_all_cand_display"
                )
                csv_exp = final_df_to_display.to_csv(index=False).encode('utf-8')
                st.download_button("Télécharger Sélection (CSV)", csv_exp, "selection_candidats.csv", "text/csv", key="dl_all_cand_sel")
            else:
                st.info("Aucun candidat ne correspond à vos filtres.")
        else:
            st.info("Aucun candidat dans la base de données.")
    except Exception as e:
        st.error(f"Erreur lors de la récupération des candidats : {e}")
    finally:
        if cursor_all_candidates:
            cursor_all_candidates.close()

# Main application logic
col1, col2, col3 = st.columns(3)

if st.session_state.page == 'home':
    st.image("https://wevii.net/wp-content/uploads/2024/05/logo-wevii-dark.svg", use_container_width=False)
    st.markdown('<div class="home-header">Assistant Wevii de Recherche de Candidats</div>', unsafe_allow_html=True)
    st.markdown('<div class="home-subheader">Comment souhaitez-vous procéder ?</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown('<div class="home-column-content">', unsafe_allow_html=True)
        st.markdown('<img src="https://img.icons8.com/ios-filled/50/1a3c6d/edit.png" alt="Formulaire"/>', unsafe_allow_html=True)
        st.markdown('<strong>Remplir un Formulaire</strong>', unsafe_allow_html=True)
        st.markdown("<p>Définissez vos critères de recherche précis via un formulaire détaillé.</p>", unsafe_allow_html=True)
        if st.button("Utiliser le formulaire", key="form_choice_home_btn", use_container_width=True):
            st.session_state.page = 'form'
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="home-column-content">', unsafe_allow_html=True)
        st.markdown('<img src="https://img.icons8.com/ios-filled/50/1a3c6d/document.png" alt="PDF"/>', unsafe_allow_html=True)
        st.markdown('<strong>Analyser une Offre (PDF)</strong>', unsafe_allow_html=True)
        st.markdown("<p>Importez une offre d'emploi au format PDF. Notre IA en extraira les informations clés.</p>", unsafe_allow_html=True)
        if st.button("Télécharger un PDF", key="file_choice_home_btn", use_container_width=True):
            st.session_state.page = 'file'
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with col3:
        st.markdown('<div class="home-column-content">', unsafe_allow_html=True)
        st.markdown('<img src="https://img.icons8.com/ios-filled/50/1a3c6d/search--v1.png" alt="Explorer"/>', unsafe_allow_html=True)
        st.markdown('<strong>Explorer Tous les Candidats</strong>', unsafe_allow_html=True)
        st.markdown("<p>Accédez à la liste complète et filtrez l'ensemble des candidats enregistrés dans notre base.</p>", unsafe_allow_html=True)
        if st.button("Voir Tous les Candidats", key="all_candidates_choice_home_btn", use_container_width=True):
            st.session_state.page = 'all_candidates'
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.page == 'file':
    cols_file_header = st.columns([0.8, 0.2])
    with cols_file_header[1]:
        if st.button("Accueil", key="back_button_file_page", help="Retourner à la page d'accueil"):
            st.session_state.page = 'home'
            st.session_state.submit_triggered = False
            st.session_state.results_ready = False
            st.session_state.workflow_completed = False
            st.session_state.request_id = None
            st.rerun()

    st.markdown('<div class="header" style="margin-top: 0px; padding-top:0px;">Assistant Wevii</div>', unsafe_allow_html=True)
    st.markdown('<div class="subheader">Analyser une Offre d\'Emploi (PDF)</div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Choisissez un fichier PDF contenant l'offre d'emploi",
        type=["pdf"],
        label_visibility="collapsed",
        key="pdf_uploader"
    )

    if uploaded_file:
        try:
            pdf_reader = PdfReader(uploaded_file)
            pdf_text_list = []
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    extracted = page.extract_text()
                    if extracted and extracted.strip():
                        pdf_text_list.append(extracted)
                except Exception as e_page:
                    st.warning(f"Avertissement lors de l'extraction de la page {page_num + 1}: {e_page}. Cette page pourrait être ignorée.")
            
            pdf_text = "\n".join(pdf_text_list)

            if not pdf_text.strip():
                extracted_text_for_display = "Aucun texte n'a pu être extrait du PDF. Le fichier est peut-être une image, protégé, ou corrompu."
                st.warning(extracted_text_for_display)
            else:
                extracted_text_for_display = pdf_text
            
            st.text_area("Texte extrait du PDF (vérifiez et modifiez si nécessaire) :", value=extracted_text_for_display, height=300, key="pdf_text_area_display")

            if st.button("Lancer l'analyse du PDF", key="submit_pdf_analysis_btn", disabled=(not pdf_text.strip() or pdf_text == "Aucun texte n'a pu être extrait du PDF. Le fichier est peut-être une image, protégé, ou corrompu.")):
                text_to_submit = st.session_state.get("pdf_text_area_display", pdf_text)

                if not text_to_submit.strip() or text_to_submit == "Aucun texte n'a pu être extrait du PDF. Le fichier est peut-être une image, protégé, ou corrompu.":
                    st.error("Impossible de soumettre : aucun texte valide à analyser.")
                else:
                    request_id = str(uuid.uuid4())
                    st.session_state.request_id = request_id
                    tables_to_clear_pdf = ["DEMANDES", "WORKFLOW_STATUS", "PROFILES", "DEMANDES_TEXTES"]
                    clear_tables(tables_to_clear_pdf)
                    
                    cursor_insert_text = None
                    try:
                        cursor_insert_text = mysql_interfaces_conn.cursor()
                        query_text = "INSERT INTO DEMANDES_TEXTES (DESCRIPTION) VALUES (%s)"
                        cursor_insert_text.execute(query_text, (text_to_submit,))
                        mysql_interfaces_conn.commit()
                        st.success("Texte de l'offre soumis pour analyse.")
                    
                        webhook_url_pdf = f"https://wevii.app.n8n.cloud/webhook/weviiApii?request_id={request_id}"
                        if send_to_webhook(webhook_url_pdf, {"pdf_text": text_to_submit}):
                            st.session_state.page = 'results'
                            st.session_state.submit_triggered = True
                            st.session_state.workflow_completed = False
                            st.rerun()
                        else:
                            st.error("Échec de l'envoi au webhook. Vérifiez l'URL ou le serveur n8n.")
                    except Exception as e_db:
                        st.error(f"Erreur base de données (DEMANDES_TEXTES) : {e_db}")
                    finally:
                        if cursor_insert_text:
                            cursor_insert_text.close()
        except Exception as e_pdf:
            st.error(f"Erreur lors de la lecture du fichier PDF : {e_pdf}. Le fichier est peut-être corrompu.")

elif st.session_state.page == 'form':
    cols_form_header = st.columns([0.8, 0.2])
    with cols_form_header[1]:
        if st.button("Accueil", key="back_button_form_page", help="Retourner à la page d'accueil"):
            st.session_state.page = 'home'
            st.session_state.submit_triggered = False
            st.session_state.results_ready = False
            st.session_state.workflow_completed = False
            st.session_state.request_id = None
            st.rerun()

    st.markdown('<div class="header" style="margin-top: 0px; padding-top:0px;">Assistant Wevii</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="subheader">Recherche de Candidats par Formulaire</div>', unsafe_allow_html=True)
    
    with st.form(key="candidate_form"):
        st.markdown("##### Informations sur le Poste")
        poste = st.text_input("Intitulé du Poste*", placeholder="ex. Développeur Python Senior")
        ville = st.text_input("Ville(s) de la mission", placeholder="ex. Lille ou Bordeaux, Paris...")
        domain = st.text_input("Domaine/Secteur d'activité", placeholder="ex. Banque, Assurance, Retail")
        
        st.markdown("##### Expérience et Compétences")
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            exp_min = st.number_input("Expérience Minimum (années)", min_value=0, step=1, value=0)
        with col_exp2:
            exp_max = st.number_input("Expérience Maximum (années)", min_value=exp_min, step=1, value=max(exp_min, 0)) 
        
        skills = st.text_area("Compétences Techniques Clés*", placeholder="ex. Python (Pandas, FastAPI), SQL (PostgreSQL), Docker, CI/CD, Cloud (AWS/Azure)...", height=100)
        environment = st.text_input("Environnement Technique/Méthodologies", placeholder="ex. Agile (Scrum), DevOps, Microservices")
        
        st.markdown("##### Autres Critères")
        language = st.selectbox("Langue(s) principale(s) pour la mission", ["Français", "Anglais", "Bilingue Français/Anglais", "Espagnol", "Allemand", "Autre"], index=0)
        description = st.text_area("Description complémentaire du profil ou de la mission", placeholder="Précisez ici d'autres aspects importants...", height=150)

        form_col_submit, form_col_spacer, form_col_reset = st.columns([2, 5, 2])
        with form_col_submit:
            submit_button = st.form_submit_button(label="Lancer la Recherche")
        with form_col_reset:
            if st.form_submit_button(label="Effacer le formulaire"):
                st.success("Formulaire prêt pour une nouvelle saisie.")

    if submit_button:
        if not poste or not skills or exp_min is None:
            st.error("Veuillez remplir au moins les champs obligatoires : Intitulé du Poste, Expérience Minimum et Compétences Techniques.")
        else:
            try:
                tables_to_clear_form = ["DEMANDES_TEXTES", "PROFILES", "DEMANDES"]
                clear_tables(tables_to_clear_form)
                
                cursor_insert_form = None
                request_id = str(uuid.uuid4())
                st.session_state.request_id = request_id
                submitted_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                try:
                    cursor_insert_form = mysql_interfaces_conn.cursor()
                    insert_query = """
                    INSERT INTO DEMANDES (
                        POSTE, EXPERIENCE_MIN, EXPERIENCE_MAX, SKILLS, ENVIRONMENT,
                        LANGUAGE, DOMAIN, DESCRIPTION, VILLE, SUBMITTEDAT, FORMMODE, REQUEST_ID
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor_insert_form.execute(insert_query, (
                        poste, exp_min, exp_max, skills, environment, language,
                        domain, description, ville, submitted_at, "form", request_id
                    ))
                    mysql_interfaces_conn.commit()
                    st.success("Critères de recherche soumis pour analyse.")

                    webhook_url_form = f"https://wevii.app.n8n.cloud/webhook/weviiApi?request_id={request_id}"
                    if send_to_webhook(webhook_url_form, {"form_data": "used"}):
                        st.session_state.page = 'results'
                        st.session_state.submit_triggered = True
                        st.session_state.workflow_completed = False
                        st.rerun()
                except Exception as e_db_form:
                    st.error(f"Erreur base de données (DEMANDES) : {e_db_form}")
                finally:
                    if cursor_insert_form:
                        cursor_insert_form.close()
            except Exception as e_main:
                st.error(f"Une erreur générale est survenue lors de la soumission : {str(e_main)}")

elif st.session_state.page == 'results':
    cols_results_header = st.columns([0.8, 0.2])
    with cols_results_header[1]:
        if st.button("Accueil", key="back_button_results_page", help="Retourner à la page d'accueil"):
            st.session_state.page = 'home'
            st.session_state.submit_triggered = False
            st.session_state.results_ready = False
            st.session_state.workflow_completed = False
            st.session_state.request_id = None
            st.rerun()
    
    st.markdown('<div class="header" style="margin-top: 0px; padding-top:0px;">Assistant Wevii</div>', unsafe_allow_html=True)
    st.markdown('<div class="subheader">Résultats de Votre Recherche</div>', unsafe_allow_html=True)

    if st.session_state.submit_triggered and not st.session_state.workflow_completed:
        poll_and_display_profiles()
    elif st.session_state.results_ready:
        display_profiles_data()
    else:
        st.info("Veuillez d'abord soumettre une recherche (via formulaire ou PDF) pour voir les résultats.")

elif st.session_state.page == 'all_candidates':
    display_all_candidates()

elif st.session_state.page == 'verify_tables':
    verify_tables()

# Footer
st.markdown("""
    <div class="footer">
        Powered by <a href="https://wevii.net" target="_blank" style="color: #DD6436; text-decoration: none;">Wevii</a>
    </div>
""", unsafe_allow_html=True)

# Close connections
mysql_interfaces_conn.close()
mysql_boondmanager_conn.close()
