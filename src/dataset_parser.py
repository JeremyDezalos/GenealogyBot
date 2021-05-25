import pandas as pd
import os
from difflib import get_close_matches

def get_relevant_columns():
    df = pd.read_excel(os.path.dirname(__file__) + "/../data/1832_pc.xlsx")
    sorter_df = df[["chef_prenom", "chef_nom", "chef_annee_naissance", "epouse_nom", "epouse_annee_naissance", "enfants_dans_la_commune_prenom", "enfants_annee_naissance"]]
    sorter_df.to_csv(os.path.dirname(__file__) + r'/../data/1832_pc_relevant_columns.csv')

    names = pd.read_excel(os.path.dirname(__file__) + "/../data/family_names.xlsx")
    names['Nom'] = names['Nom'].str.lower()
    filtered_names = names[["Nom"]].drop_duplicates().reset_index(drop=True)
    filtered_names.to_csv(os.path.dirname(__file__) + r'/../data/all_names_only.csv')

    prenoms = pd.read_csv(os.path.dirname(__file__) + "/../data/all_prenoms.csv")
    prenoms['prenom'] = prenoms['prenom'].str.lower()
    filtered_prenoms = prenoms[["prenom"]].drop_duplicates().reset_index(drop=True)
    filtered_prenoms.to_csv(os.path.dirname(__file__) + r'/../data/all_prenoms_only.csv')

def throw_away_bad_names(db, names, prenoms):
    filter = db.chef_nom.isin(names.Nom)


    db.loc[:,'name_is_correct'] = filter
    db = db.query('name_is_correct == True')
    del db['name_is_correct']

    filter = db.chef_prenom.isin(prenoms.prenom)
    db.loc[:,'name_is_correct'] = filter
    db = db.query('name_is_correct == True')
    del db['name_is_correct']

    filter1 = db.epouse_nom.isin(names.Nom)
    filter2 = db.epouse_nom.isin(prenoms.prenom)
    db.loc[:,'name_is_correct'] = filter1
    db.loc[:,'prenom_is_correct'] = filter2
    db = db.query('name_is_correct == True or prenom_is_correct == True or epouse_nom == "·"')

    del db['name_is_correct']
    del db['prenom_is_correct']
    db = db.dropna(subset=['epouse_nom'])
    return db

def transform_field_to_int(db, field):
    db[field] = db[field].apply(lambda x: x if x.isdigit() else 0) # enleve les '.'
    db[field] = db[field].apply(lambda x: int(x)) # met tout en int
    db[field] = db[field].apply(lambda x: 0 if x < 1730 or x > 1833 else x) #sélectionne les bonnes dates
    return db



def transform_dates_to_int(db):
    db = transform_field_to_int(db, 'chef_annee_naissance')
    db = transform_field_to_int(db, 'epouse_annee_naissance')
    return db

def filter_children(db):
    filter = db.enfants_dans_la_commune_prenom.isin(prenoms.prenom)
    db.loc[:,'name_is_correct'] = filter
    db = db.query('name_is_correct == True')
    del db['name_is_correct']
    db["annee_enfant"] = db["annee_enfant"].apply(lambda x: x if str(x).isdigit() else 0) # enleve les '.'
    db["annee_enfant"] = db["annee_enfant"].apply(lambda x: int(x)) # met tout en int
    db["annee_enfant"] = db["annee_enfant"].apply(lambda x: 0 if x < 1730 or x > 1833 else x) #sélectionne les bonnes dates
    db = db[db["annee_enfant"] != 0]
    return db

def create_child_list(db):
    chef = db[["chef_prenom","chef_nom", "epouse_nom"]]
    enfant = db["enfants_dans_la_commune_prenom"].str.split("|")
    chef_enfant = chef.join(enfant).explode("enfants_dans_la_commune_prenom").reset_index()[["chef_prenom","chef_nom", "epouse_nom", "enfants_dans_la_commune_prenom"]]
    annee = db["enfants_annee_naissance"].str.split("|")
    annee2 = annee.explode().reset_index()[["enfants_annee_naissance"]]
    chef_enfant["annee_enfant"]=annee2
    return chef_enfant

#get_relevant_columns() #doing this once is enough
db = pd.read_csv(os.path.dirname(__file__) + "/../data/1832_pc_relevant_columns.csv")
del db['Unnamed: 0']
names = pd.read_csv(os.path.dirname(__file__) + "/../data/all_names_only.csv")
prenoms = pd.read_csv(os.path.dirname(__file__) + "/../data/all_prenoms_only.csv")
filtered_db = throw_away_bad_names(db, names, prenoms)
filtered_db = transform_dates_to_int(filtered_db)
filtered_db.to_csv(os.path.dirname(__file__) + r'/../data/filtered.csv')
child_db = create_child_list(filtered_db)
print(child_db)
filter_children(child_db).to_csv(os.path.dirname(__file__) + r'/../data/chef_enfant.csv')