# Use multi-stage builds to manage different environments
ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

# Base image for Python
FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# Base image for OpenJDK
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

# Copy Python binaries from the Python image
COPY --from=py3 / /

# Install PySpark
ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

# Create a directory for your application
WORKDIR /app

# Copy the dataset to the container
# Assuming the dataset does not change frequently
COPY ./dataset /app/dataset

# Copy only the application code
# This is separated to leverage Docker's cache for the steps above
COPY query.py /app/
COPY extract.py /app/

# Set the command to run your application
CMD ["python", "query.py"]
