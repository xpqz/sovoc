# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
sudo add-apt-repository -y ppa:jonathonf/python-3.6
sudo apt-get update -yq
sudo apt-get install -yq build-essential libreadline-dev libsqlite3-dev libssl-dev sqlite3 python3.6 python3.6-venv python3.6-dev unixodbc unixodbc-dev couchdb
SCRIPT

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/xenial64"
  config.vm.provision 'shell', inline: $script
end
