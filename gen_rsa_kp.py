#!/usr/bin/env python
"""
This module generates a RSA key pair for communicating encrypted data to and
from the Lambda. The Lambda's private key is stored in an ENV variable on
Lambda; a future TODO is to make this more secure with Amazon KMS.
"""

from Crypto.PublicKey import RSA

from shared.crypto import PRIVATE_KEY_ENV_VAR

PRIVATE_KEY_FILE = 'lambda.private.txt'
PUBLIC_KEY_FILE = 'lambda.public.pem'

privateKey = RSA.generate(2048)
publickey = privateKey.publickey()

print 'Writing private key to', PRIVATE_KEY_FILE
with open(PRIVATE_KEY_FILE, 'wb') as ofs:
    ofs.write(privateKey.exportKey('DER').encode('hex'))

print 'Writing public key to', PUBLIC_KEY_FILE
with open(PUBLIC_KEY_FILE, 'wb') as ofs:
    ofs.write(publickey.exportKey('PEM'))

print "Done! Now, add the contents of %s to your lambda's environment " \
      "as %s or let the deploy script do it for you" % (
    PRIVATE_KEY_FILE, PRIVATE_KEY_ENV_VAR)
