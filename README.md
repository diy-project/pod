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
2. Create an IAM role for lambda (requires S3, SQS).
3. Run `collect.sh` from inside `./lambda` and then `./deploy.sh`.
4. Execute `main.py`.
5. For best results, install an adblocker, setup a S3 bucket for
large payload transfers, enable MITM, and run with the command 
`./main.py -m -s3 <bucket_name>`.
6. Set the browser's proxy configuration (by default: localhost:1080)

#### [Optional] MITM proxy
- To use the MITM (HTTPS) proxy, run `gen_cert.sh` to
generate a root CA certificate.
- Install this into your least favorite (non-banking) browser.
- Execute `main.py -m`.

#### [Optional] Running in full-local mode
- Execute `main.py -l`.

