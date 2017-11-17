#!/bin/sh

SCRIPTDIR=`dirname '$BASH_SOURCE'`

# Install requirements
pip install requests pycryptodome -t $SCRIPTDIR

# Copy shared libraries to the Lambda
cp -r $SCRIPTDIR/../shared $SCRIPTDIR/

zip proxy.zip -x '*.pyc' -r \
    certifi \
    chardet \
    idna \
    requests \
    urllib3 \
    Crypto \
    shared \
    *-info \
    impl \
    proxy.py
