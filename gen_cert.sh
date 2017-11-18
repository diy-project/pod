#!/bin/bash 
# This script generates the root CA key and certificate that will be used
# by the proxy to issue new certificates on the fly when interfacing with
# web browsers. Add the certificate to the browser's certificate store.

SUBJECT="/C=US/ST=California/L=Palo Alto/O=Stanford University/OU=DIY Project/CN=Lambda MITM Proxy"

openssl genrsa -out mitm.key.pem 4096
openssl req -x509 -new -nodes -key mitm.key.pem -sha256 -days 30 -out mitm.ca.pem -subj "$SUBJECT"
