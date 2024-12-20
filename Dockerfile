FROM python:3.12

# RUN apt-get update
# RUN apt-get install -y gconf-service libasound2 libatk1.0-0 libcairo2 libcups2 libfontconfig1 libgdk-pixbuf2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libxss1 fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils

# RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV NAME Johnny
ENV PORT 5001
# ENV APP_ENV PRODUCTION
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . .
COPY svc_acc_key.json $APP_HOME/svc_acc_key.json

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

