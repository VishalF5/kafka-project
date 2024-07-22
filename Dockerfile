# Use the official Ubuntu base image
FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install Python and pip
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment
RUN python3 -m venv /python
ENV PATH="/python/bin:$PATH"

# Ensure pip is up-to-date
RUN pip3 install --upgrade pip

# Copy all files from the current directory to /app in the container
COPY . /kafka-project

# Set the working directory to /app
WORKDIR /kafka-project
RUN mkdir logs

# Install Python dependencies within the virtual environment
RUN pip3 install --no-cache-dir -r requirements.txt

# Make sure the script is executable
RUN chmod +x run.sh

# Set the entrypoint to your script and use the virtual environment's Python
CMD ["/bin/bash", "run.sh"]
