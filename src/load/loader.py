import luigi
from .chinook_load import LoadChinook
from .database_load import LoadDatabase
from .disaster_load import LoadDisaster
from .review_load import LoadReview
from .tweet_load import LoadTweet
from .user_load import LoadUser

class LuigiLoader(luigi.Task):
    """
    This class extend luigi task for transforming and 
    loading All data to SQLite warehouse database

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

        return [LoadChinook(), LoadDatabase(),
                LoadDisaster(), LoadReview(),
                LoadTweet(), LoadUser()]

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
