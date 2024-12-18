import pandas as pd
import os


class DataLoader:
    def __init__(self):
        # Caricamento dei file CSV esistenti
        self.movies_df = pd.read_csv('../../dataset/movies.csv').set_index('id')
        self.actors_df = pd.read_csv('../../dataset/actors.csv').set_index('id')
        self.countries_df = pd.read_csv('../../dataset/countries.csv').set_index('id')
        self.crew_df = pd.read_csv('../../dataset/crew.csv').set_index('id')
        self.genres_df = pd.read_csv('../../dataset/genres.csv').set_index('id')
        self.languages_df = pd.read_csv('../../dataset/languages.csv').set_index('id')
        self.posters_df = pd.read_csv('../../dataset/posters.csv').set_index('id')
        self.releases_df = pd.read_csv('../../dataset/releases.csv').set_index('id')
        self.studios_df = pd.read_csv('../../dataset/studios.csv').set_index('id')
        self.themes_df = pd.read_csv('../../dataset/themes.csv').set_index('id')

        # Carica il CSV Rotten Tomatoes e imposta un ID univoco concatenando 'rotten_tomatoes_link', 'critic_name' e 'review_date'
        self.rotten_tomatoes_df = pd.read_csv('../../dataset/rotten_tomatoes_reviews.csv')

        # Creiamo un identificatore univoco concatenando 'rotten_tomatoes_link', 'critic_name' e 'review_date'
        self.rotten_tomatoes_df['review_id'] = (
                self.rotten_tomatoes_df['rotten_tomatoes_link'] +
                "_" + self.rotten_tomatoes_df['critic_name'].fillna('Unknown') +
                "_" + self.rotten_tomatoes_df['review_date'].fillna('Unknown')
        )

        # Impostiamo 'review_id' come indice
        self.rotten_tomatoes_df = self.rotten_tomatoes_df.set_index('review_id')

        self.oscar_awards_df = pd.read_csv('../../dataset/the_oscar_awards.csv')

        # Creiamo un identificatore univoco concatenando 'year_film', 'category' e 'name'
        self.oscar_awards_df['oscar_id'] = (
                self.oscar_awards_df['year_film'].astype(str) +
                "_" + self.oscar_awards_df['category'] +
                "_" + self.oscar_awards_df['name']
        )

        # Impostiamo 'oscar_id' come indice
        self.oscar_awards_df = self.oscar_awards_df.set_index('oscar_id')

    # Metodo per ottenere i dati
    def get_data(self):
        return self

    # Setter per aggiornare i DataFrame
    def set_movies_df(self, movies_df):
        self.movies_df = movies_df

    def set_actors_df(self, actors_df):
        self.actors_df = actors_df

    def set_countries_df(self, countries_df):
        self.countries_df = countries_df

    def set_crew_df(self, crew_df):
        self.crew_df = crew_df

    def set_genres_df(self, genres_df):
        self.genres_df = genres_df

    def set_languages_df(self, languages_df):
        self.languages_df = languages_df

    def set_posters_df(self, posters_df):
        self.posters_df = posters_df

    def set_releases_df(self, releases_df):
        self.releases_df = releases_df

    def set_studios_df(self, studios_df):
        self.studios_df = studios_df

    def set_themes_df(self, themes_df):
        self.themes_df = themes_df

    def set_rotten_tomatoes_df(self, rotten_tomatoes_df):
        self.rotten_tomatoes_df = rotten_tomatoes_df

    def set_oscar_awards_df(self, oscar_awards_df):
        self.oscar_awards_df = oscar_awards_df

    # Metodo per salvare tutti i DataFrame in file CSV
    def _save_data(self):
        # Controlla se la cartella cleaned esiste già
        cleaned_path = '../../dataset/cleaned'
        if not os.path.exists(cleaned_path):
            os.makedirs(cleaned_path)

        # Salva i DataFrame in file CSV
        self.movies_df.to_csv(f'{cleaned_path}/movies.csv')
        self.actors_df.to_csv(f'{cleaned_path}/actors.csv')
        self.countries_df.to_csv(f'{cleaned_path}/countries.csv')
        self.crew_df.to_csv(f'{cleaned_path}/crew.csv')
        self.genres_df.to_csv(f'{cleaned_path}/genres.csv')
        self.languages_df.to_csv(f'{cleaned_path}/languages.csv')
        self.posters_df.to_csv(f'{cleaned_path}/posters.csv')
        self.releases_df.to_csv(f'{cleaned_path}/releases.csv')
        self.studios_df.to_csv(f'{cleaned_path}/studios.csv')
        self.themes_df.to_csv(f'{cleaned_path}/themes.csv')
        self.rotten_tomatoes_df.to_csv(f'{cleaned_path}/rotten_tomatoes_reviews.csv')
        self.oscar_awards_df.to_csv(f'{cleaned_path}/the_oscar_awards.csv')

        from sqlalchemy import create_engine, text
        import pandas as pd
        import os
        import glob

        # Configurazione del database
        DB_HOST = "localhost"
        DB_PORT = 5432
        DB_NAME = "your_database"
        DB_USER = "movies"
        DB_PASSWORD = "Frigorifero" #METTETE LA VOSTRA PASSWORD

        # Directory dei CSV
        CSV_DIR = "./normalized_tables/"

        # Crea l'engine di connessione
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

        # Funzione per caricare i dati in una tabella
        def load_csv_to_table(csv_path, table_name):
            try:
                # Leggi il CSV
                df = pd.read_csv(csv_path)

                # Carica i dati nel database
                df.to_sql(table_name, engine, if_exists="append", index=False)
                print(f"Dati caricati nella tabella '{table_name}' con successo.")

            except Exception as e:
                print(f"Errore durante il caricamento nella tabella '{table_name}':", e)

        # Popola tutte le tabelle
        def populate_tables():
            try:
                # Trova tutti i file CSV nella directory
                for csv_file in glob.glob(os.path.join(CSV_DIR, "*.csv")):
                    # Determina il nome della tabella dal nome del file
                    table_name = os.path.basename(csv_file).replace(".csv", "")

                    # Carica i dati dal CSV nella tabella
                    load_csv_to_table(csv_file, table_name)

                print("Tutte le tabelle sono state popolate con successo!")

            except Exception as e:
                print("Errore generale:", e)

        # Esegui lo script
        if __name__ == "__main__":
            populate_tables()
