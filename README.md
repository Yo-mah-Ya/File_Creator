# create a specified format file by a specified file

### set up poetry

1. first

Create virtual environment which means that you create .venv folder under the project directory.

```shell
poetry config --local --list
poetry config --local virtualenvs.in-project true
```

check virtual environment is activated

```shell
poetry env list
```

2. second

create `pyproject.toml`

```
poetry init
```

3. third

add packages

```shell
poetry add --dev pytest autopep8 flake8
poetry add pandas
```

4. test

```shell
# run with coverage
poetry run pytest -v --cov=tests --cov-branch
# run withou coverage
poetry run pytest -vv
```
