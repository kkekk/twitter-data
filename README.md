# twitter-data

Modeling [twitter's stream](https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter.html) using neo4j.

### Prerequisites

Ensure [neo4j](https://neo4j.com/download/) is installed on your machine.
This project uses python 3.6.6, Jupyter Notebooks and `Pipenv`.
*note to self - Pipenv is garbage slow. Use [poetry](https://github.com/sdispater/poetry) to manage virtualenvs next time.*

### Installing

If possible, use `pyenv` to manage python versions. [Link here.](https://github.com/pyenv/pyenv)
Install [Pipenv](https://pipenv.readthedocs.io/en/latest/install/) (installation instructions differ per machine).

To setup the virtual environment:
```
Pipenv install -r requirements.txt
```
Launch the shell:
```
Pipenv shell
```
The notebooks can now be accessed via `jupyter notebook`.
To run the scraper, enter:
```
python scraper.py
```
with the shell activated. Thats it!
