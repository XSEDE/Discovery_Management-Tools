#!/bin/bash

### RePublish Tool
MY_BASE=/soft/warehouse-apps-1.0/Management-Tools

PYTHON=python3
PYTHON_BASE=/soft/python/python-3.7.7-base
PYTHON_ROOT=/soft/warehouse-apps-1.0/Management-Tools/python
source ${PYTHON_ROOT}/bin/activate

export PYTHONPATH=$DAEMON_DIR/lib:/soft/warehouse-1.0/PROD/django_xsede_warehouse
export DJANGO_CONF=/soft/warehouse-apps-1.0/Management-Tools/conf/django_xsede_warehouse.conf
export DJANGO_SETTINGS_MODULE=xsede_warehouse.settings

$MY_BASE/PROD/bin/es_reload.py -c $MY_BASE/conf/es_reload.conf "$@"
RETVAL=$?
echo rc=$RETVAL
exit $RETVAL
