#!/usr/bin/env python3
import sqlite3
import luigi
import pandas as pd
import time
import json

year, month, day = map(str, time.strftime("%Y %m %d").split())
timestamp = year+""+month+""+day


class ChinookData(luigi.Task):
    """
    This class extend luigi task for
    extracting ChinookData

    Attributes
    ----------
    local_target : str
        input file target name
    output_target : str
        output file target name
    task_complete : boolean
        status of luigi task
    """

    local_target = timestamp+"_chinook_{}.csv"
    output_target = "./processed/"+local_target
    task_complete = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ['albums', 'artists', 'customers', 'employees',
                        'genres', 'invoice_items', 'invoices', 'media_types', 'playlist_track', 'playlists', 'sqlite_sequence',
                        'sqlite_stat1', 'tracks']

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            Empty list
        """

        return []

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method load data from the local SQLite database
        and write CSV file represent the database
        """

        con = sqlite3.connect("./sources/chinook.db")

        for column in self.columns:
            results = pd.read_sql_query('SELECT * from {}'.format(column), con)
            results.to_csv(self.output_target.format(column),
                           encoding='utf-8', index=False, header=True, quoting=2)

        self.task_complete = True


class DatabaseData(luigi.Task):
    """
    This class extend luigi task for
    extracting DatabaseData

    Attributes
    ----------
    local_target : str
        input file target name
    output_target : str
        output file target name
    task_complete : boolean
        status of luigi task
    """

    local_target = timestamp+"_database_{}.csv"
    output_target = "./processed/"+local_target
    task_complete = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ['artists', 'content', 'genres',
                        'labels', 'reviews', 'years']

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            Empty list
        """

        return []

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method load data from the local SQLite database
        and write CSV file represent the database
        """

        con = sqlite3.connect("./sources/database.sqlite")

        for column in self.columns:
            results = pd.read_sql_query('SELECT * from {}'.format(column), con)
            results.to_csv(self.output_target.format(column),
                           encoding='utf-8', index=False, header=True, quoting=2)

        self.task_complete = True


class DisasterData(luigi.Task):
    """
    This class extend luigi task for
    extracting DisasterData

    Attributes
    ----------
    local_target : str
        input file target name
    output_target : str
        output file target name
    task_complete : boolean
        status of luigi task
    """

    local_target = timestamp+"_disaster_data.csv"
    output_target = "./processed/"+local_target
    task_complete = False

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            Empty list
        """

        return []

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method load data from the local CSV files
        and write another CSV file represent the previous one
        """

        df = pd.read_csv('./sources/disaster_data.csv')

        df.to_csv(self.output_target,
                  encoding='utf-8', index=False, header=True, quoting=2)

        self.task_complete = True


class ReviewData(luigi.Task):
    """
    This class extend luigi task for
    extracting ReviewData

    Attributes
    ----------
    local_target : str
        input file target name
    output_target : str
        output file target name
    task_complete : boolean
        status of luigi task
    """

    local_target = timestamp+"_reviews_data.csv"
    output_target = "./processed/"+local_target
    task_complete = False

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            Empty list
        """

        return []

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method load data from different local CSV files
        and write another CSV file represent the previous one
        """

        review1 = pd.read_csv('./sources/reviews_q1.csv')
        review2 = pd.read_csv('./sources/reviews_q2.csv')
        review3 = pd.read_csv('./sources/reviews_q2.csv')
        review4 = pd.read_excel('./sources/reviews_q1.xlsx')

        frames = [review1, review2, review3, review4]

        df = pd.concat(frames)
        df.drop_duplicates(subset=['id'], inplace=True)

        df.to_csv(self.output_target,
                  encoding='utf-8', index=False, header=True, quoting=2)

        self.task_complete = True


class TweetData(luigi.Task):
    """
    This class extend luigi task for
    extracting TweetData

    Attributes
    ----------
    local_target : str
        input file target name
    output_target : str
        output file target name
    task_complete : boolean
        status of luigi task
    """

    local_target = timestamp+"_tweets_data.csv"
    output_target = "./processed/"+local_target
    task_complete = False

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            Empty list
        """

        return []

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method load data from the local JSON files
        and write CSV file represent the previous one
        """

        df = pd.DataFrame(columns=['id', 'text', 'lang', 'country', 'user'])

        with open('./sources/tweet_data.json') as f:
            for line in f:
                data = json.loads(line)

                df2 = pd.DataFrame([[data['id'], data['text'], data['lang'],
                                     data['user']['location'],
                                     data['user']['screen_name']]],
                                   columns=['id', 'text', 'lang', 'country', 'user'])

                df = pd.concat([df2, df])

        df.to_csv(self.output_target,
                  encoding='utf-8', index=False, header=True, quoting=2)

        self.task_complete = True


class UserData(luigi.Task):
    """
    This class extend luigi task for
    extracting UserData

    Attributes
    ----------
    local_target : str
        input file target name
    output_target : str
        output file target name
    task_complete : boolean
        status of luigi task
    """

    local_target = timestamp+"_users_data.csv"
    output_target = "./processed/"+local_target
    task_complete = False

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            Empty list
        """

        return []

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method load data from the local Excel files
        and write CSV file represent the previous one
        """

        df = pd.read_excel('./sources/file_1000.xls')
        df.drop(['Unnamed: 0', 'First Name.1'], axis=1, inplace=True)

        df.to_csv(self.output_target,
                  encoding='utf-8', index=False, header=True, quoting=2)

        self.task_complete = True


class ExtractData(luigi.Task):
    """
    This class extend luigi task
    to execute all class in this file

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    """

    task_complete = False

    def requires(self):
        """
        This method will be executed before
        the run method

        Returns
        ----------
        list : list
            List of all class to be executed
            before the run method
        """

        return [ChinookData(), DatabaseData(),
                DisasterData(), ReviewData(),
                TweetData(), UserData()]

    def complete(self):
        """
        This method will be executed after
        the run method

        Returns
        ----------
        task_complete : boolean
            status of luigi task
        """

        return self.task_complete

    def run(self):
        """
        This method will be executed as a parent method
        to all class in this file
        """

        self.task_complete = True
