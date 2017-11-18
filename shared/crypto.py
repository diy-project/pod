"""
Note: this file will be copied to the Lambda too. Do not
add dependencies carelessly.
"""
from Crypto.Cipher import AES, PKCS1_OAEP


PRIVATE_KEY_ENV_VAR = 'RSA_PRIVATE_KEY'

# Since a new symmetric key is generated for each request, we can hard code
# these nonces.
REQUEST_NONCE = 'request'
S3_REQUEST_NONCE = 's3request'
RESPONSE_NONCE = 'response'
S3_RESPONSE_NONCE = 's3response'


def encrypt_with_gcm(key, cleartext, nonce):
    cipher = AES.new(key, AES.MODE_GCM, nonce)
    ciphertext, tag = cipher.encrypt_and_digest(cleartext)
    return ciphertext, tag


def decrypt_with_gcm(key, ciphertext, tag, nonce):
    cipher = AES.new(key, AES.MODE_GCM, nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)
