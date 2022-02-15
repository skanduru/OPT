#!/usr/bin/env bash

apt update
apt install -y openssh-client less
git config --global user.name "First Last"
git config --global user.mail "me@example.com"
git remote remove origin
git remote add origin git@github.com:mygit/myrepo.git
git push --set-upstream origin master
conda install -y pytest

<< ENV
Check the following:
1. faust -A faust_stream worker -l info
2. python consumers/ksql.py should work
   DROP TABLE <table>
   TERMINATE <query_name>
ENV
