FROM python:3.11

# Install linux libraries
RUN apt-get update && apt-get install -y wget unzip libnetcdf-dev
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up versioning
ARG version_number
ARG commit_sha
ENV VERSION_NUMBER=$version_number
ENV COMMIT_SHA=$commit_sha
LABEL COMMIT_SHA=$commit_sha
LABEL VERSION_NUMBER=$version_number

# Install Korona (must have the LSSS zip file in the local drive)
COPY lsss-3.1.0-alpha-20250411-1120-linux.zip /
RUN unzip lsss-3.1.0-alpha-20250411-1120-linux.zip
RUN rm lsss-3.1.0-alpha-20250411-1120-linux.zip
COPY KoronaCli.sh /lsss-3.1.0-alpha/korona/KoronaCli.sh
RUN chmod 777 /lsss-3.1.0-alpha/korona/KoronaCli.sh

# Install python libraries & python code
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install wheel --upgrade --force-reinstall && \
    pip install --no-cache-dir -r requirements.txt

COPY CRIMAC_createlabels.py /app/CRIMAC_createlabels.py

WORKDIR /app

CMD ["sh", "-c", "python3 -u /app/CRIMAC_createlabels.py > /griddedout/log_annotation.txt 2>&1"]
