AWS Lambda HTTP(S) Proxy
========================
This implements a proxy using AWS Lambda for anonymous browsing or crawling.

Usage instructions
------------------

#### Prerequisites
- A web browser
- OpenSSL installed (to generate root CA certs)
- AWS credentials setup with `aws configure`

#### Running the proxy
1. Install the dependencies in `requirements.txt`.
2. Create an IAM role for lambda (requires S3, SQS).
3. Run `collect.sh` from inside `./lambda` and then `./deploy.sh`.
4. Execute `main.py -f <function_name>`.
5. For best results, install an adblocker, setup a S3 bucket for
large payload transfers, enable MITM, and run with the command 
`./main.py -m -f <function_name> -s3 <bucket_name>`.
6. Set the browser's proxy configuration (by default: localhost:1080)

#### [Optional] MITM proxy
The daemon acts as a Man-in-the-Middle, generating certificates on the fly
for requests. When proxying requests, the daemon will handle certificate
validation in place of the browser.
- To use the MITM (HTTPS) proxy, run `gen_cert.sh` to
generate a root CA certificate.
- Install this into your least favorite (non-banking) browser.
- Execute `main.py -m`.

#### [Optional] Running in full-local mode
All requests are proxied locally for debugging purposes.
- Execute `main.py -l`.

#### [Optional] Encrypt data to the lambda
By default, data to the lambda is encrypted with TLS. However, this means
that all parts of the AWS stack that the data traverses can see in plaintext
request and response headers and bodies. This option allows encryption of
request and response data so that only the daemon on localhost and the lambda 
(or an attacker with access to the lambda's environment variables) can decrypt
it. 
- Execute `gen_rsa_kp.py`.
- Set the private key as RSA_PRIVATE_KEY in the lambda's env
- Run `main.py` with `-e`.

