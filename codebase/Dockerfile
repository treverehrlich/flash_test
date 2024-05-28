FROM 823367020558.dkr.ecr.us-east-1.amazonaws.com/codebase_base@sha256:00a6b5a3d1bdb0e1e46504c09a62900ec71759c71837535a044c08ff887a8bb2

## DaVinci Codebase Install
# Minimal files with requirements to cache pip install layer
COPY ./requirements.txt codebase/requirements.txt
COPY ./setup.py codebase/setup.py
COPY ./README.md codebase/README.md
RUN python3 -m pip install -e codebase

# Copy the rest of codebase files
COPY . ./codebase

## Setup prod indicators
ARG PROD_FLAG
ENV DAVINCI_PROD=$PROD_FLAG

# Entrypoint
CMD ["python"]