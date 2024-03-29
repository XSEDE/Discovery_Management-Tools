#!/bin/bash
#this is only for amie.xsede.org, that is, the source of the amie packets
#coming through RabbitMQ.  All other amie users should use create_amie_user.sh

input=$(sed -r 's/street=/2.5.4.9=/g' <<<"$1")
input=$(sed -r 's/postalCode=/2.5.4.17=/g' <<<"$input")
echo creating user $input
sudo rabbitmqctl add_user "$input" foo

sudo rabbitmqctl list_user_permissions "$1"

sudo rabbitmqctl set_permissions -p xsede "$1" "^amq.gen.*" "^amq.gen.*|^amie.to.*" "^amq.gen.*|^amie.from.*
