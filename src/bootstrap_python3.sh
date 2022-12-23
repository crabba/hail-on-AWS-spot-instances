#!/bin/bash
set -e

# 221123 crabba My fork of this script, updated as python36 is not available
# This file is available in public bucket: s3://hms-dbmi-docs/hail_bootstrap/bootstrap_python3.sh

export PATH=$PATH:/usr/local/bin

cd $HOME
mkdir -p $HOME/.ssh/id_rsa
sudo yum install -y python38 python38-devel python38-setuptools python38-pip
# sudo easy_install pip
# sudo python3 -m pip install --upgrade pip

PACKAGES=$(cat <<END
argparse
bokeh
boto3
ipywidgets
numpy
oauth
pandas
parsimonious
pyserial
python-magic
requests
scipy
utils
wheel
END
)

if grep isMaster /mnt/var/lib/info/instance.json | grep true; then
    sudo yum install -y g++ cmake git gcc-c++
    sudo yum install -y lz4 lz4-devel
	# Master node: Install all
	PACKAGES="${PACKAGES}
	jupyterlab"
fi

for package in $PACKAGES
do
	echo "Installing pip package: ${package}"
	sudo python3 -m pip install $package
done


# Ref: https://www.codeammo.com/article/install-phantomjs-on-amazon-linux/
# Install phantomjs-2.1.1 for bokeh.export_png
# yum install fontconfig freetype freetype-devel fontconfig-devel libstdc++
# wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2
# sudo mkdir -p /opt/phantomjs
# bzip2 -d phantomjs-2.1.1-linux-x86_64.tar.bz2
# sudo tar -xvf phantomjs-2.1.1-linux-x86_64.tar \
#     --directory /opt/phantomjs/ --strip-components 1
# sudo ln -s /opt/phantomjs/bin/phantomjs /usr/bin/phantomjs

sudo yum update -y # It has to be at the end so it does not interfere with other yum installations
