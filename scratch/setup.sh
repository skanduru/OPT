#!/usr/bin/env bash

conda install -y pytest &
apt update
apt install -y openssh-client less
git config --global user.name "Srinivasa Kanduru"
git config --global user.mail "skanduru@gmail.com"
git remote remove origin
git remote add origin git@github.com:skanduru/OPT.git
git push --set-upstream origin master

<< ENV
Check the following:
1. faust -A faust_stream worker -l info
2. python consumers/ksql.py should work
   DROP TABLE <table>
   TERMINATE <query_name>
ENV
