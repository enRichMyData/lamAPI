# Use the specified Python version
FROM python:3.11

# Set the working directory
WORKDIR /app

# Copy project metadata and source code needed for installation
COPY pyproject.toml ./
COPY lamapi ./lamapi
COPY app ./api

RUN pip install --no-cache-dir -e .

# Install SpaCy
RUN pip install spacy

# Download SpaCy model
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download en_core_web_trf

ENV NLTK_DATA=/usr/local/nltk_data
RUN python -m nltk.downloader -d /usr/local/nltk_data punkt stopwords

# Copy the rest of the project (scripts, configs, etc.)
COPY . .
