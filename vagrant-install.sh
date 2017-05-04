#!/bin/bash

set -e

sudo apt-get update -yq
sudo apt-get install -yq build-essential
  
# sqlite3 via apt-get is too old
cd /tmp
wget https://sqlite.org/2017/sqlite-autoconf-3180000.tar.gz
tar zxf sqlite-autoconf-3180000.tar.gz
cd sqlite-autoconf-3180000
./configure --prefix=/usr --enable-json1
sudo make install
cd && sudo rm -rf /tmp/sqlite-autoconf-3180000 /tmp/sqlite-autoconf-3180000.tar.gz

# python3 via apt-get is too old
# sudo apt-get install -yq libreadline-dev libsqlite3-dev libssl-dev
sudo apt-get install -yq libreadline-dev libssl-dev

echo 'Install Python 3.6...'
cd /tmp
wget -O- https://www.python.org/ftp/python/3.6.1/Python-3.6.1.tgz | tar xz
cd Python-3.6.1
./configure
make
sudo make altinstall

cd && sudo rm -rf /tmp/Python-3.6.1

echo 'Done!'

cd /vagrant
/usr/local/bin/python3.6 -m venv ./py3
