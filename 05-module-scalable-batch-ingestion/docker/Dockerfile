
# Docker image of the current PySpark processing container with spark 3.2, python 3.9 and optimized for CPU
FROM 173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.2-cpu-py39-v1.0

# Upgrade pip 
RUN pip3 install --upgrade pip

# Install the sagemaker feature store pyspark connector 
RUN pip3 install sagemaker-feature-store-pyspark-3.2 --no-binary :all: --verbose

# Send logs direct to the terminal
ENV PYTHONUNBUFFERED=TRUE

# Set the entrypoint 
ENTRYPOINT [ "smspark-submit", "processing.py" ]
