# Use the specified Python version
ARG PYTHON_VERSION
FROM python:3.11

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY pyproject.toml .

RUN pip install --no-cache-dir -e .

# Install SpaCy
RUN pip install spacy

# Download SpaCy model
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download en_core_web_trf

ENV NLTK_DATA=/usr/local/nltk_data
RUN python -m nltk.downloader -d /usr/local/nltk_data punkt stopwords

# Copy the rest of the application code
COPY . .
