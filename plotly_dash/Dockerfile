# Get the python image
FROM python:3.9

# Set Dash debug mode to off
ENV DASH_DEBUG_MODE False

# Move the files to the Docker image
COPY ./app.py /app.py
COPY ./requirements.txt /requirements.txt

# Set the working directory 
WORKDIR /

# Install the dependencies
RUN set -ex && \
    pip install -r requirements.txt

# Expose the Dash port
EXPOSE 8050

# Run the Python script
CMD ["python", "app.py"]