%!/bin/sh

SCRIPTDIR=`dirname ''$BASH_SOURCE'`
pip install requests -t $SCRIPTDIR

zip proxy.zip -r \
    certifi \
    chardet \
    idna \
    requests \
    urllib3 \
    *-info \
    proxy.py
