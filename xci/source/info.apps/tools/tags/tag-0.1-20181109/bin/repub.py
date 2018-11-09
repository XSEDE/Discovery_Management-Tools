#!/usr/bin/env python
##################################################################################
# Republish a glue2.EntityHistory record to RabbitMQ
#     Can update the exchange
#     Can update the about (resourceid)
#
# Date: November 9, 2018
# Author: JP Navarro
##################################################################################
import amqp
import pprint
import os
import pwd
import re
import sys
import argparse
import datetime
from datetime import datetime, tzinfo, timedelta
from time import sleep
try:
    import http.client as httplib
except ImportError:
    import httplib
import json
import csv
import ssl
import shutil
import pdb

import django
django.setup()
from django.utils.dateparse import parse_datetime
from django.utils.encoding import uri_to_iri
from glue2_db.models import EntityHistory

class UTC(tzinfo):
    def utcoffset(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return 'UTC'
    def dst(self, dt):
        return timedelta(0)
utc = UTC()

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class RePub():
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-i', '--id', action='store', dest='id', required=True, \
                            help='EntityHistory ID')
        parser.add_argument('-c', '--config', action='store', default='./conf/repub.conf', \
                            help='Configuration file default=./repub.conf')
        parser.add_argument('-e', '--exchange', action='store', dest='exchange', \
                            help='Exchange to publish to')
        parser.add_argument('-a', '-about', '-r', '--resourceid', action='store', dest='about', \
                            help='Replacement ResourceID (about)')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()
        
        if self.args.pdb:
            pdb.set_trace()

        try:
            self.ID = int(self.args.id)
        except:
            eprint('Missing or invalid ID argument')
            sys.exit(1)
        
        # Load configuration file
        config_file = os.path.abspath(self.args.config)
        try:
            with open(config_file, 'r') as file:
                conf=file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as ex:
            eprint('Error "{}" parsing config={}'.format(ex, config_file))
            sys.exit(1)
                            
    def ConnectAmqp_UserPass(self):
        ssl_opts = {'ca_certs': os.environ.get('X509_USER_CERT')}
        for host in [self.config['AMQP_PRIMARY'], self.config['AMQP_FALLBACK']]:
            try:
                eprint('AMQP connecting to host={} as userid={}'.format(host, self.config['AMQP_USERID']))
                conn = amqp.Connection(host=host, virtual_host='xsede',
                                   userid=self.config['AMQP_USERID'], password=self.config['AMQP_PASSWORD'],
                                   ssl=ssl_opts)
                conn.connect()
                channel = conn.channel()
                return channel
            except Exception as ex:
                 eprint('AMQP connect error: ' + format(ex))
        eprint('Failed to connect to all AMQP services')
        sys.exit(1)
                     
    def RetrieveHistory(self, id):
        try:
            model = EntityHistory.objects.get(pk=uri_to_iri(id))
        except EntityHistory.DoesNotExist:
            eprint('EntityHistory ID={} not found'.format(id))
            return(False)
        return(model)

    def Publish(self, object):
        self.channel = self.ConnectAmqp_UserPass()
        self.channel.basic_publish(amqp.Message(body=json.dumps(object.EntityJSON)),
                                   exchange=self.args.exchange or object.DocumentType.encode("utf-8"),
                                   routing_key=self.args.about or object.ResourceID.encode("utf-8"))
                            
if __name__ == '__main__':
    me = RePub()
    history_object = me.RetrieveHistory(me.ID)
    if not history_object:
        sys.exit(1)
    result = me.Publish(history_object)
    sys.exit(result)
