Pour exécuter le code Ia, vous devez installer les bibliothèques Python suivantes. 
Voici la liste des dépendances nécessaires, basées sur les importations et l'utilisation dans le code :

time : Bibliothèque standard de Python, aucune installation requise.
pandas : Pour la manipulation des DataFrames.

Installation : pip install pandas


sentence_transformers : Pour les modèles de transformation de phrases (MiniLM et DistilBERT).

Installation : pip install sentence-transformers


numpy : Pour les calculs numériques et la manipulation des tableaux.

Installation : pip install numpy


sqlalchemy : Pour les connexions et requêtes à la base de données MySQL.

Installation : pip install sqlalchemy


mysql-connector-python : Driver pour connecter SQLAlchemy à MySQL.

Installation : pip install mysql-connector-python


flask : Pour créer l'API web.

Installation : pip install flask

#########################################################
Commande pour installer toutes les bibliothèques nécessaires :
pip install pandas sentence-transformers numpy sqlalchemy mysql-connector-python flask
#########################################################

Pour exécuter le code Streamlit fourni, vous devez installer les bibliothèques Python suivantes, basées sur les importations et l'utilisation dans le code :

streamlit : Pour créer l'interface web interactive.

Installation : pip install streamlit


mysql-connector-python : Pour se connecter à la base de données MySQL.

Installation : pip install mysql-connector-python


pandas : Pour la manipulation des DataFrames.

Installation : pip install pandas


requests : Pour envoyer des requêtes HTTP vers le webhook.

Installation : pip install requests


time : Bibliothèque standard de Python, aucune installation requise.
uuid : Bibliothèque standard de Python, aucune installation requise.
pypdf : Pour lire et extraire le texte des fichiers PDF.

Installation : pip install pypdf

#########################################################
Commande pour installer toutes les bibliothèques nécessaires:
pip install streamlit mysql-connector-python pandas requests pypdf
#########################################################

Pour que tu puisses tester le projet exposé via ngrok, voici les étapes que j'ai suivies pour le configurer, ainsi que les informations nécessaires :

Télécharge et installe ngrok.
Une fois installé, décompresse l'archive et place le fichier ngrok.

Authentification avec mon compte :

J'utilise un compte gratuit. tu peux créer ton propre compte gratuit et obtenir un token via le tableau de bord ngrok.
Exécute cette commande dans ton terminal pour t'authentifier :
ngrok authtoken <ton_token>

Lancement de ngrok :

le projet (Code IA) tourne localement sur le port 5050 (dans ma configuration). Pour exposer ce port, lance ngrok avec la commande suivante dans ton terminal :
ngrok http 5050

-- Cela créera une URL publique temporaire (par exemple, https://<quelquechose>.ngrok-free.app) que tu pourras utiliser pour accéder à l'application.

Vérification :

Une fois ngrok lancé, tu verras une sortie dans le terminal avec l'URL de forwarding (par exemple, https://8c1c-176-175-194-1.ngrok-free.app -> http://localhost:5050).


