#!/urs/bin/env python

from Cryptodome.PublicKey import RSA

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

print "Done! Now, add the contents of %s to your lambda's environment as %s" % (
    PRIVATE_KEY_FILE, PRIVATE_KEY_ENV_VAR)
