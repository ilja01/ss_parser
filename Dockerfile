# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the needed files into the container
COPY main.py ./
COPY db_creds.csv ./
COPY telegram_creds.csv ./
COPY requirements.txt ./    

# Install the needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Run script.py when the container launches
ENTRYPOINT ["python", "./main.py"]