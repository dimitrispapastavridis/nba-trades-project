# set the base image with specific tag/version
FROM python:3.9

# update all packages, install cron module and the text editors vim and nano 
RUN apt-get update && apt-get -y install cron 
RUN apt-get -y install vim nano

# set up working directory inside the container
WORKDIR /app

# copy and run the requirements.txt file to install the required packages.
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# copy files from  directory to the image's filesystem
COPY . .

RUN mkdir csv
# register a cron job to start the webscraper application
# it needs the absolute paths (runnning 'which python' inside the container might be useful)
RUN crontab -l | { cat; echo "* * * * * /usr/local/bin/python /app/nba-trades.py"; } | crontab -

# start cron in foreground and set it as executable command for when the container starts 
CMD ["cron", "-f"]    