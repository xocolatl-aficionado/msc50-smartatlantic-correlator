# Use the official Python image from the Docker Hub
FROM python:3.11-slim

# Set environment variables
ENV PIP_NO_CACHE_DIR=1

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create and set the working directory
WORKDIR /app

# Copy the requirements.txt file and install dependencies
COPY requirements.txt .

COPY coords_dict.pkl .


RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the Streamlit port
EXPOSE 8501

# Run Streamlit
ENTRYPOINT ["streamlit", "run"]
CMD ["smart-data-coorelator.py"]
