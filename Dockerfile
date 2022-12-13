FROM python:3.9-slim as python-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.4 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

######################################################

# `builder-base` stage is used to build deps + create our virtual environment
FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        # deps for installing poetry
        curl \
        # deps for building python deps
        build-essential
        # deps for psycopg2
#        libpq-dev

# install poetry - respects $POETRY_VERSION & $POETRY_HOME
#RUN curl -sSL https://raw.githubusercontent.com/zipdrug/poetry/master/get-poetry.py | python
RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.1.15

# set poetry in PATH
#ENV PATH="${PATH}:/root/.poetry/bin:${POETRY_HOME}"

# copy project requirement files here to ensure they will be cached.
WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./

# install runtime deps - uses $POETRY_VIRTUALENVS_IN_PROJECT internally
RUN poetry install --no-dev
#RUN source $HOME/.poetry/env && poetry update && poetry install --no-dev

# `development` image is used during development / testing
FROM python-base as development
ENV RUN_ENV=development
WORKDIR $PYSETUP_PATH

# copy in our built poetry + venv
COPY --from=builder-base $POETRY_HOME $POETRY_HOME
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH

# quicker install as runtime deps are already installed
RUN poetry install

COPY src/lgs_removezipcode /app
# will become mountpoint of our code
WORKDIR /app
CMD ["python","main.py"]
CMD ["python","topic_setup.py"]


# `production` image used for runtime
FROM python-base as production
ENV RUN_ENV=production
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
COPY src/lgs_removezipcode /app
WORKDIR /app
CMD ["python","main.py"]
CMD ["python","topic_setup.py"]
