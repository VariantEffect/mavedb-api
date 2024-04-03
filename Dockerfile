################################
# python-base
# Set up shared environment variables
################################
FROM python:3.9 as python-base

    # Poetry
    # https://python-poetry.org/docs/configuration/#using-environment-variables
ENV POETRY_VERSION=1.7.0 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    # never create virtual environments, only use the venv prepared by us
    POETRY_VIRTUALENVS_CREATE=false \
    \
    # this is where our requirements + virtual environment will live
    VIRTUAL_ENV="/venv"

# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VIRTUAL_ENV/bin:$PATH"

# prepare virtual env
RUN python3 -m venv $VIRTUAL_ENV

# Python path
ENV PYTHONPATH="$PYTHONPATH:/code/src"


################################
# downloader
# Set up large downloaded files
################################
FROM python-base AS downloader

WORKDIR /data

# Install tools necessary used to install samtools and htslib so we can configure fasta files for genomic assembly.
RUN apt-get clean && apt-get update && apt-get install -y \
	build-essential \
	curl \
	git \
	libbz2-dev \
	libcurl4-openssl-dev \
	libgsl0-dev \
	liblzma-dev \
	libncurses5-dev \
	libperl-dev \
	libssl-dev \
	zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install samtools and htslib.
ARG htsversion=1.19
RUN curl -L https://github.com/samtools/htslib/releases/download/${htsversion}/htslib-${htsversion}.tar.bz2 | tar xj && \
    (cd htslib-${htsversion} && ./configure --enable-plugins --with-plugin-path='$(libexecdir)/htslib:/usr/libexec/htslib' && make install) && \
    ldconfig && \
    curl -L https://github.com/samtools/samtools/releases/download/${htsversion}/samtools-${htsversion}.tar.bz2 | tar xj && \
    (cd samtools-${htsversion} && ./configure --with-htslib=system && make install) && \
    curl -L https://github.com/samtools/bcftools/releases/download/${htsversion}/bcftools-${htsversion}.tar.bz2 | tar xj && \
    (cd bcftools-${htsversion} && ./configure --enable-libgsl --enable-perl-filters --with-htslib=system && make install)

# Fetch and index GRCh37 and GRCh38 assemblies.
RUN wget -O - https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/all_assembly_versions/GCF_000001405.25_GRCh37.p13/GCF_000001405.25_GRCh37.p13_genomic.fna.gz | gzip -d | bgzip >  GCF_000001405.25_GRCh37.p13_genomic.fna.gz
RUN wget -O - https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/all_assembly_versions/GCF_000001405.39_GRCh38.p13/GCF_000001405.39_GRCh38.p13_genomic.fna.gz | gzip -d | bgzip > GCF_000001405.39_GRCh38.p13_genomic.fna.gz
RUN samtools faidx GCF_000001405.25_GRCh37.p13_genomic.fna.gz
RUN samtools faidx GCF_000001405.39_GRCh38.p13_genomic.fna.gz

################################
# builder
# Builds application dependencies and creates venv
################################
FROM python-base as builder

WORKDIR /code

# install poetry - respects $POETRY_VERSION & $POETRY_HOME
RUN curl -sSL https://install.python-poetry.org | python3 -

# initialize dependencies
COPY poetry.lock pyproject.toml ./

# installs runtime dependencies to $VIRTUAL_ENV
RUN poetry install --no-root --extras server
COPY src /code/src
COPY src/mavedb/server_main.py /code/main.py

################################
# application
# Application image
################################
FROM builder as application
COPY --from=downloader /data /data

# copy pre-built poetry + venv
COPY --from=builder $POETRY_HOME $POETRY_HOME
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

# Generate a self-signed certificate. This Docker image is for use behind a load balancer or other reverse proxy, so it
# can be self-signed and does not need a real domain name.
RUN mkdir -p /code/ssl
RUN openssl req -nodes -x509 \
    -newkey rsa:4096 \
    -sha256 \
    -days 730 \
    -keyout /code/ssl/server.key \
    -out /code/ssl/server.cert \
    -subj "/C=US/ST=Washington/L=Seattle/O=University of Washington/OU=Brotman Baty Institute/CN=mavedb-api"

# Tell Docker that we will listen on port 8000.
EXPOSE 8000

# At container startup, run the application using uvicorn.
CMD ["uvicorn", "mavedb.server_main:app", "--host", "0.0.0.0", "--port", "8000"]

################################
# worker
# Worker image
################################
FROM builder as worker
COPY --from=downloader /data /data

# copy pre-built poetry + venv
COPY --from=builder $POETRY_HOME $POETRY_HOME
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

CMD ["arq", "mavedb.worker.WorkerSettings"]
