# As Scrapy runs on Python, I run the official Python 3 Docker image.
FROM python:3.9.7-slim

# Install libpq-dev for psycopg2 python package and cron
RUN apt-get update && apt-get -y install libpq-dev gcc
RUN apt-get -y install cron

# Copy project in app folder
COPY . /app
# Set the working directory /app.
WORKDIR /app

# Copy cron file to the cron.d directory
COPY cron-script /etc/cron.d/cron-script
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cron-script
# Apply cron job
RUN crontab /etc/cron.d/cron-script
# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run venv
RUN python3 -m venv /opt/venv

# Install packages from requirements.txt.
RUN /opt/venv/bin/pip install pip --upgrade && \
    /opt/venv/bin/pip install -r requirements.txt

# Update Pythonpath
RUN echo 'export PYTHONPATH=$PYTHONPATH:/app' >> ~/.bashrc
RUN echo 'export PYTHONPATH=$PYTHONPATH:/app/isin-price' >> ~/.bashrc

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log
#CMD ["cd /app/"]
#CMD ["/opt/venv/bin/python3", "./app.py"]
