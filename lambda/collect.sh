#!/bin/sh

SCRIPTDIR=`dirname '$BASH_SOURCE'`

# Install requirements
pip3 install requests==2.29.0 pycryptodome  -t $SCRIPTDIR

# Copy shared libraries to the Lambda
cp -r $SCRIPTDIR/../shared $SCRIPTDIR/

zip proxy.zip -x '*.pyc' -r \
    certifi \
    charset_normalizer \
    idna \
    requests \
    urllib3 \
    Crypto \
    shared \
    *-info \
    impl \
    proxy.py
