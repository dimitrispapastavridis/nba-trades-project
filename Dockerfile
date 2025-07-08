FROM python:3.9

RUN apt-get update && apt-get -y install cron 
RUN apt-get -y install vim nano

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .

RUN mkdir csv

RUN crontab -l | { cat; echo "* * * * * /usr/local/bin/python /app/nba-trades.py"; } | crontab -

CMD ["cron", "-f"]    
