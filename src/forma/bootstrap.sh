#!/bin/bash
# 
# EMR bootstrap task for our gdal stuff.

set -e

bucket=reddconfig
fwtools=FWTools-linux-x86_64-4.0.0.tar.gz
native=linuxnative.tar.gz
jblas=libjblas.tar.gz
sources=/etc/apt/sources.list
hadoop_lib=/home/hadoop/native/Linux-amd64-64

# Start with screen.
sudo apt-get -y --force-yes install screen
sudo apt-get -y --force-yes install exim4

# Install htop - helpful for monitoring slave nodes
sudo apt-get -y --force-yes install htop

# Install libhdf4
sudo aptitude -fy install libhdf4-dev

# Install FWTools, (GDAL 1.8.0)
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$fwtools
sudo mkdir -p /usr/local/fwtools
sudo tar -C /usr/local/fwtools --strip-components=2 -xvzf $fwtools
sudo chown --recursive hadoop /usr/local/fwtools

# Download native Java bindings for gdal and jblas
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$native
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$jblas

# Untar everything into EMR's native library path
sudo tar -C $hadoop_lib --strip-components=2 -xvzf $native
sudo tar -C $hadoop_lib --strip-components=1 -xvzf $jblas
sudo chown --recursive hadoop $hadoop_lib

# Add proper configs to hadoop-env.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$hadoop_lib:\$LD_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-user-env.sh
echo "export JAVA_LIBRARY_PATH=$hadoop_lib:\$JAVA_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-user-env.sh

# Add to bashrc, for good measure.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$hadoop_lib:\$LD_LIBRARY_PATH" >> /home/hadoop/.bashrc
echo "export JAVA_LIBRARY_PATH=$hadoop_lib:\$JAVA_LIBRARY_PATH" >> /home/hadoop/.bashrc

# Convenient 'repl' command
echo "alias repl='screen -Lm hadoop jar /home/hadoop/forma-clj/forma-0.2.0-SNAPSHOT-standalone.jar clojure.main'" >> /home/hadoop/.bashrc

# Setup for git
sudo apt-get -y --force-yes install git
# mkdir /home/hadoop/.ssh

echo 'INSERT SSH PRIVATE KEY HERE - THERE HAS TO BE A BETTER WAY TO USE GIT ON CLUSTER - CORRECT KEY IS IN S3 VERSION' > /home/hadoop/.ssh/id_rsa

# fix permissions
chmod 600 /home/hadoop/.ssh/id_rsa

# add public key
echo 'INSERT PUBLIC KEY HERE - THERE HAS TO BE A BETTER WAY TO USE GIT ON CLUSTER - CORRECT KEY IS IN S3 VERSION' > /home/hadoop/.ssh/id_rsa.pub

# Add github to known_hosts
echo "github.com,207.97.227.239 ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" >> /home/hadoop/.ssh/known_hosts

cd /home/hadoop/
source /home/hadoop/.bashrc

# Install lzo support
sudo apt-get -y --force-yes install liblzo2-dev

# Build lzo support for Hadoop
git clone https://github.com/kevinweil/hadoop-lzo.git
cd hadoop-lzo
export CFLAGS=-m64
export CXXFLAGS=-m64
ant compile-native tar

# Move lzo files into place
cd build
cp hadoop-lzo-*.jar /home/hadoop/lib/
cp -r native/Linux-amd64-64/* /home/hadoop/native/
cd /home/hadoop

# Install lein
cd bin
wget https://raw.github.com/technomancy/leiningen/stable/bin/lein
chmod u+x lein

# Bootstrap lein
lein
cd ..

# Get forma repo - easier to edit, test code
git clone git@github.com:reddmetrics/forma-clj.git
cd forma-clj
git checkout develop

# Now ready to use!
lein deps
lein uberjar

exit 0

