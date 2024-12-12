import pandas as pd
import os

class DataLoader:
    def __init__(self):
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

    def get_data(self):
        return self

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

    def _save_data(self):
        os.mkdir('../../dataset/cleaned')
        self.movies_df.to_csv('../../dataset/cleaned/movies.csv')
        self.actors_df.to_csv('../../dataset/cleaned/actors.csv')
        self.countries_df.to_csv('../../dataset/cleaned/countries.csv')
        self.crew_df.to_csv('../../dataset/cleaned/crew.csv')
        self.genres_df.to_csv('../../dataset/cleaned/genres.csv')
        self.languages_df.to_csv('../../dataset/cleaned/languages.csv')
        self.posters_df.to_csv('../../dataset/cleaned/posters.csv')
        self.releases_df.to_csv('../../dataset/cleaned/releases.csv')
        self.studios_df.to_csv('../../dataset/cleaned/studios.csv')
        self.themes_df.to_csv('../../dataset/cleaned/themes.csv')