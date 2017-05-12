#!/bin/bash

set -e

# Git
mkdir /home/ubuntu/sovoc
git clone --mirror /vagrant /home/ubuntu/sovoc/.git
cd /home/ubuntu/sovoc
git config --bool core.bare false
git checkout master

# Python
/usr/bin/python3.6 -m venv ./py3
source py3/bin/activate
pip install --upgrade pip
pip install wheel
pip install aioodbc
pip install gevent gevent-websocket gunicorn wsaccel ujson
pip install Flask

# SQLite driver for odbc
cd /tmp
wget http://www.ch-werner.de/sqliteodbc/sqliteodbc-0.9995.tar.gz
tar zxf sqliteodbc-0.9995.tar.gz 
cd sqliteodbc-0.9995
./configure
make
sudo make install

cat > odbcinst.ini << EOF
[SQLite3]
Description=SQLite ODBC Driver
Driver=/usr/local/lib/libsqlite3odbc.so
Setup=/usr/local/lib/libsqlite3odbc.so
Threading=4
EOF

sudo mv odbcinst.ini /etc

# Couch stuff
curl -HContent-Type:application/json -XPUT 'http://localhost:5984/_users/org.couchdb.user:stefan' --data-binary '{"_id": "org.couchdb.user:stefan","name": "stefan","roles": [],"type": "user","password": "xyzzy"}'
