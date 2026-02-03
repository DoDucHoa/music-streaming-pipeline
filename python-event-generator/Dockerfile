FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and data
COPY event_generator.py .
COPY data/*.gz ./data/

# Remove problematic user agents file if exists
RUN rm -f ./data/user\ agents.txt || true

# Run the generator
CMD ["python", "-u", "event_generator.py"]
