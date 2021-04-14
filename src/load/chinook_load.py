import time
import pandas as pd
import luigi
import sys
from pathlib import Path
file = Path(__file__). resolve()
package_root_directory = file.parents[1]
sys.path.append(str(package_root_directory))

from sqlite import SqliteConnection
from luigi_preprocessed import ExtractData

year, month, day = map(str, time.strftime("%Y %m %d").split())
timestamp = year+""+month+""+day
file_path = "./processed/"+timestamp


class LoadChinookArtists(luigi.Task):
    """
    This class extend luigi task for transforming 
    and loading Artist data from ChinookData 
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_artists"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()
        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookFullTracks(luigi.Task):
    """
    This class extend luigi task for transforming and loading
    Albums, Tracks, Genres, and MediaTypes data from 
    ChinookData to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_tracksalbums"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        albums = pd.read_csv(file_path+'_chinook_albums.csv')
        tracks = pd.read_csv(file_path+'_chinook_tracks.csv')
        genres = pd.read_csv(file_path+'_chinook_genres.csv')
        mediatypes = pd.read_csv(file_path+'_chinook_media_types.csv')

        df = pd.merge(albums,
                      pd.merge(mediatypes,
                               pd.merge(tracks, genres,
                                        suffixes=(None, '_genre'),
                                        how="inner", on="GenreId"),
                               suffixes=('_mediatype', None),
                               how="inner", on="MediaTypeId"),
                      how="inner", on="AlbumId", sort=True)

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookCustomers(luigi.Task):
    """
    This class extend luigi task for transforming 
    and loading Customers data from ChinookData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_customers"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')
        df['FullName'] = df['FirstName'] + " " + df['LastName']
        df.drop(['FirstName', 'LastName', 'Company',
                'State', 'Fax'], axis=1, inplace=True)

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookEmployees(luigi.Task):
    """
    This class extend luigi task for transforming 
    and loading Employees data from ChinookData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_employees"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')
        df['fullname'] = df['FirstName'] + " " + df['LastName']
        df['BirthDate'] = df['BirthDate'].apply(lambda x: x.split(" 00")[0])
        df['HireDate'] = df['HireDate'].apply(lambda x: x.split(" 00")[0])

        df.drop(['FirstName', 'LastName', 'Fax'], axis=1, inplace=True)

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookInvoiceItems(luigi.Task):
    """
    This class extend luigi task for transforming and 
    loading InvoiceItems data from ChinookData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_invoice_items"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')
        df.drop(['UnitPrice', 'Quantity'], axis=1, inplace=True)

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookInvoices(luigi.Task):
    """
    This class extend luigi task for transforming
    and loading Invoices data from ChinookData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_invoices"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')
        df['InvoiceDate'] = df['InvoiceDate'].apply(
            lambda x: x.split(" 00")[0])

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookPlaylists(luigi.Task):
    """
    This class extend luigi task for transforming
    and loading Playlists data from ChinookData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_playlists"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinookPlaylistTracks(luigi.Task):
    """
    This class extend luigi task for transforming
    and loading PlaylistTracks data from ChinookData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "chinook_playlist_track"

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

        return [ExtractData()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        con = SqliteConnection()

        df = pd.read_csv(file_path+'_' + self.table_name + '.csv')

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True


class LoadChinook(luigi.Task):
    """
    This class extend luigi task for transforming
    and loading All data from ChinookData
    to SQLite warehouse database

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

        return [LoadChinookFullTracks(), LoadChinookArtists(),
                LoadChinookCustomers(), LoadChinookEmployees(),
                LoadChinookInvoiceItems(), LoadChinookInvoices(),
                LoadChinookPlaylists(), LoadChinookPlaylistTracks()]

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
        This method transform and load data
        to SQlite warehouse database
        """

        self.task_complete = True
