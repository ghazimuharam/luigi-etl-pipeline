# Luigi ETL Pipeline

Luigi ETL Pipeline is a simple Data Pipeline that consolidates data from multiple sources and transforms them into the data warehouse(SQlite) by using [Luigi package](https://github.com/spotify/luigi).

Luigi ETL Pipeline running demo

![run_luigi](https://user-images.githubusercontent.com/22569688/114757241-cd916080-9d85-11eb-95b8-4cabbaa40ad2.gif)

## Installation

Use git to clone this repository

```bash
git clone https://github.com/ghazimuharam/luigi-etl-pipeline.git
```

## Prerequisite

Make sure you have python 3.7 installed on your machine

```bash
> python --version
Python 3.7.10
```

To run the script in this repository, you need to install the prerequisite library from requirements.txt

```bash
pip install -r requirements.txt
```

## Usage

Before running the main program, you need to make sure Luigi is running on your machine, to run Luigi, run the command below

```bash
luigid
```

Run the data pipeline with the command below

```bash
python main.py
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
