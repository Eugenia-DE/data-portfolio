# Dockerfile for Python data generator
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir faker pandas

COPY data_generator.py .

# Run the data generator 
CMD ["python", "data_generator.py"]