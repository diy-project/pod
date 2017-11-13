AWS Lambda HTTP(S) Proxy
========================
This implements a proxy using AWS Lambda for anonymous browsing.

Usage instructions
------------------
1. Install the dependencies in `requirements.txt`.
2. Run `collect.sh` from inside `./lambda` and upload the zipfile
to AWS Lambda.
3. Execute `main.py`.

#### [Optional] MITM Proxy
4. To use the MITM (HTTPS) proxy (not-recommended), run `gen_cert.sh` to
generate a root CA certificate.
5. Install this into your least favorite browser.
6. Execute `main.py -m`.

#### [Optional] Running in full-local mode
7. Execute `main.py -l -m`.

