FROM python:3.11

# Install linux libraries
RUN apt-get update && apt-get install -y wget unzip libnetcdf-dev zsh

# Set up versioning
ARG version_number
ARG commit_sha
ENV VERSION_NUMBER=$version_number
ENV COMMIT_SHA=$commit_sha
LABEL COMMIT_SHA=$commit_sha
LABEL VERSION_NUMBER=$version_number




# Install python libraries & python code
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
COPY CRIMAC_makelabelszarr.py /app/CRIMAC_makelabelszarr.py
COPY annotationtools /app/annotationtools



WORKDIR /app

CMD ["sh", "-c", "python3 -u /app/CRIMAC_makelabelszarr.py >> /dataout/log_annotation.txt 2>&1"]