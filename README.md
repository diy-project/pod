Proxy-On-Demand: A serverless HTTP(S) proxy
===========================================
An on-demand, browser-compatible, proxy backed by AWS 
Lambda for anonymous browsing and web crawling. For normal web browsing,
Proxy-On-Demand (POD) costs fractions of cents to operate and is price
competitive with tiny EC2 instances, fitting entirely in the free-tier.
For heavy loads, POD provides scalability in real-time,
streaming up to 4K videos on youtube.

![alt text](screenshot.png 'Run the proxy with live stats.')

Usage instructions
------------------

#### Prerequisites
- A web browser (tested with Firefox)
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

Optional steps
--------------

#### MITM proxy
The daemon acts as a Man-in-the-Middle, generating certificates on the fly
for requests. When proxying requests, the daemon will handle certificate
validation in place of the browser.
- To use the MITM (HTTPS) proxy, run `gen_cert.sh` to
generate a root CA certificate.
- Install this into your least favorite (non-banking) browser.
- Execute `main.py -m`.

#### Running in full-local mode
All requests are proxied locally for debugging purposes.
- Execute `main.py -l`.

#### Encrypt data to the lambda
By default, data to the lambda is encrypted with TLS. However, this means
that all parts of the AWS stack that the data traverses can see in plaintext
request and response headers and bodies. This option allows encryption of
request and response data so that only the daemon on localhost and the lambda 
(or an attacker with access to the lambda's environment variables) can decrypt
it. 
- Execute `gen_rsa_kp.py`.
- Set the private key as RSA_PRIVATE_KEY in the lambda's env
- Run `main.py` with `-e`.

#### Providing multiple functions
- If the `-f` flag is specified multiple times, then the multiple functions
will be registered.
- To register functions in regions other than the default region, you must use
the function's full arn.
- Note: functions in different regions may lead to high billing rates for S3.
