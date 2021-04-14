import time
import pandas as pd
import luigi
import sys
from pathlib import Path
file = Path(__file__). resolve()
package_root_directory = file.parents[1]
sys.path.append(str(package_root_directory))

from luigi_preprocessed import ExtractData
from sqlite import SqliteConnection

year, month, day = map(str, time.strftime("%Y %m %d").split())
timestamp = year+""+month+""+day
file_path = "./processed/"+timestamp


class LoadReview(luigi.Task):
    """
    This class extend luigi task for transforming and 
    loading All listing review data from ReviewData
    to SQLite warehouse database

    Attributes
    ----------
    task_complete : boolean
        status of luigi task
    table_name : str
        table_name in SQlite warehouse database
    """

    task_complete = False
    table_name = "listing_reviews"

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

        df = pd.read_csv(file_path+'_reviews_data.csv')

        df.to_sql(self.table_name, con.connection(),
                  if_exists="replace", index=False)

        con.close()

        self.task_complete = True
