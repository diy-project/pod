AWS Lambda HTTP(S) Proxy
========================
This implements a proxy using AWS Lambda for anonymous browsing.

Usage instructions
------------------

#### Prerequisites
- A web browser
- OpenSSL installed (to generate root CA certs)
- AWS credentials setup with `aws configure`

#### Running the proxy
1. Install the dependencies in `requirements.txt`.
2. Run `collect.sh` from inside `./lambda` and upload the zipfile
to AWS Lambda. Grant the function SQS permissions.
3. Execute `main.py`.
4. For best results, install an adblocker, setup a S3 bucket for 
large payload transfers, enable MITM, and run with the command 
`./main.py -m -s3 <bucket_name>`.

#### [Optional] MITM proxy
5. To use the MITM (HTTPS) proxy (not-recommended), run `gen_cert.sh` to
generate a root CA certificate.
6. Install this into your least favorite browser.
7. Execute `main.py -m`.

#### [Optional] Running in full-local mode
8. Execute `main.py -l -m`.

