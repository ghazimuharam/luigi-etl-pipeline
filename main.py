#!/usr/bin/env python3
import luigi
from src.load import LuigiLoader

"""luigi etl pipeline 
This program does Extract, Transform, Load to given data
using luigi (https://github.com/spotify/luigi)
"""

__author__ = "Muhammad Ghazi Muharam"
__version__ = "0.1.0"
__license__ = "MIT"


def main():
    """
    Main method of luigi-etl-pipeline
    """
    luigi.run(main_task_cls=LuigiLoader, local_scheduler=False)


if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()
