#!/usr/bin/env python3
#
# Reload ElasticSearch Index
#
import argparse
from collections import Counter
#import datetime
from datetime import datetime, timezone, timedelta
from hashlib import md5
import http.client as httplib
import json
import logging
import logging.handlers
import os
from pid import PidFile
import pwd
import re
import shutil
import signal
import ssl
import sys, traceback
from time import sleep
from urllib.parse import urlparse
import pytz
Central = pytz.timezone("US/Central")

import django
django.setup()
from django.conf import settings as django_settings
from django.db import DataError, IntegrityError
from django.forms.models import model_to_dict
from resource_v3.models import *
from processing_status.process import ProcessingActivity

import elasticsearch_dsl.connections
from elasticsearch_dsl import Index
from elasticsearch import Elasticsearch, RequestsHttpConnection

import pdb

# Used during initialization before loggin is enabled
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class Router():
    # Initialization BEFORE we know if another self is running
    def __init__(self):
        # Parse arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', dest='config', required=True, \
                            help='Configuration file')
        parser.add_argument('-g', '--group', action='store', dest='group', \
                            help='Resource Group')
        parser.add_argument('-t', '--type', action='store', dest='type', \
                            help='Resource Type')
        parser.add_argument('-a', '--affiliation', action='store', dest='affiliation', \
                            help='Resource Affiliation')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        # Trace for debugging as early as possible
        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        config_path = os.path.abspath(self.args.config)
        try:
            with open(config_path, 'r') as file:
                conf=file.read()
        except IOError as e:
            eprint('Error "{}" reading config={}'.format(e, config_path))
            sys.exit(1)
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            eprint('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        if self.config.get('PID_FILE'):
            self.pidfile_path =  self.config['PID_FILE']
        else:
            name = os.path.basename(__file__).replace('.py', '')
            self.pidfile_path = '/var/run/{}/{}.pid'.format(name, name)
            
    # Setup AFTER we know that no other self is running
    def Setup(self, peak_sleep=10, offpeak_sleep=60, max_stale=24 * 60):
        # Initialize log level from arguments, or config file, or default to WARNING
        loglevel_str = (self.args.log or self.config.get('LOG_LEVEL', 'WARNING')).upper()
        loglevel_num = getattr(logging, loglevel_str, None)
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(loglevel_num)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', \
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], \
            when='W6', backupCount=999, utc=True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Signal handling
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)

        self.logger.info('Starting program={}, pid={}, uid={}({})'.format(os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        # Connect Database
        configured_database = django_settings.DATABASES['default'].get('HOST', None)
        if configured_database:
            self.logger.info('Warehouse database={}'.format(configured_database))
        # Django connects automatially as needed

        # Connect Elasticsearch
        if 'ELASTIC_HOSTS' not in self.config:
            router.logger.error('Elasticsearch not configured, exiting')
            sys.exit(1)
            
        self.logger.info('Warehouse elastichost={}'.format(self.config['ELASTIC_HOSTS']))
        self.ESEARCH = elasticsearch_dsl.connections.create_connection( \
            hosts = self.config['ELASTIC_HOSTS'], \
            connection_class = RequestsHttpConnection, \
            timeout = 10)

        self.WAREHOUSE_CATALOG = 'ResourceV3'
        
        self.want = []
        if self.args.group:
            self.want_group = set(self.args.group.split(','))
            self.want.append('group')
            self.logger.info('Selected group(s)={}'.format(','.join(self.want_group)))
        if self.args.type:
            self.want_type = set(self.args.type.split(','))
            self.want.append('type')
            self.logger.info('Selected types(s)={}'.format(','.join(self.want_type)))
        if self.args.affiliation:
            self.want_affiliation = set(self.args.affiliation.split(','))
            self.want.append('affiliation')
            self.logger.info('Selected affiliation(s)={}'.format(','.join(self.want_affiliation)))
        if not self.want:   # If nothing selected, we are rebuilding the entire index
            self.want.append('all')
            self.logger.info('Selected ALL resources')
            self.logger.info('Deleting and reloading entire Elasticsearch ResourceV3Index')
            elasticsearch_dsl.Index(ResourceV3Index.Index.name).delete(ignore=404)

        # Initialize it in case we deleted it or it never existed
        ResourceV3Index.init()

        self.total = 0

    def exit_signal(self, signum, frame):
        self.logger.critical('Caught signal={}({}), exiting with rc={}'.format(signum, signal.Signals(signum).name, signum))
        sys.exit(signum)
        
    def exit(self, rc):
        if rc:
            self.logger.error('Exiting with rc={}'.format(rc))
        sys.exit(rc)

########## CUSTOMIZATIONS START ##########

    def Run(self):
        loop_start_utc = datetime.now(timezone.utc)
        
        allRELATIONS = {}
        for rel in ResourceV3Relation.objects.all():
            if rel.FirstResourceID not in allRELATIONS:
                allRELATIONS[rel.FirstResourceID] = {}
            allRELATIONS[rel.FirstResourceID][rel.SecondResourceID] = rel.RelationType
        
        # Initial querie
        if self.want[0] == 'all':
            objects = ResourceV3.objects.all()
        elif self.want[0] == 'group':
            objects = ResourceV3.objects.filter(ResourceGroup__in=self.want_group)
        elif self.want[0] == 'type':
            objects = ResourceV3.objects.filter(Type__in=self.want_type)
        elif self.want[0] == 'affiliation':
            objects = ResourceV3.objects.filter(Affiliation__in=self.want_affiliation)
        # Additional queries
        for q in self.want[1:]:
            if q == 'group':
                objects = objects.filter(ResourceGroup__in=self.want_group)
            elif q == 'type':
                objects = objects.filter(Type__in=self.want_type)
            elif q == 'affiliation':
                objects = objects.filter(Affiliation__in=self.want_affiliation)

        for item in objects:
            myNEWRELATIONS = allRELATIONS.get(item.ID, {})
#            myNEWRELATIONS = {}
#            for rel in ResourceV3Relation.objects.filter(FirstResourceID=item.ID):
#                myNEWRELATIONS[rel.SecondResourceID] = rel.RelationType
            item.indexing(myNEWRELATIONS)
            self.total += 1
        self.logger.info('Index reload count={} duration={:.3f}/seconds'.format(self.total, (datetime.now(timezone.utc) - loop_start_utc).total_seconds()))
        return(0)

########## CUSTOMIZATIONS END ##########

if __name__ == '__main__':
    router = Router()
    with PidFile(router.pidfile_path):
        try:
            router.Setup()
            rc = router.Run()
        except Exception as e:
            msg = '{} Exception: {}'.format(type(e).__name__, e)
            router.logger.error(msg)
            traceback.print_exc(file=sys.stdout)
            rc = 1
    router.exit(rc)
