#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p certs
printf 'changeit\n' > certs/password.txt
# Reuse existing lab identities; deleting them intentionally creates a new lab CA.
if [ -f certs/truststore.p12 ]; then echo "Certificates already exist in certs/"; exit 0; fi
# The password is public classroom data. Never use it for a real system.
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -keyout certs/ca.key -out certs/ca.crt -subj /CN=KafkaLabCA
keytool -importcert -noprompt -alias ca -file certs/ca.crt -keystore certs/truststore.p12 -storetype PKCS12 -storepass changeit
for identity in localhost admin alice bob; do
    keytool -genkeypair -alias "$identity" -keyalg RSA -keysize 2048 -dname "CN=$identity" -validity 365 -keystore "certs/$identity.p12" -storetype PKCS12 -storepass changeit -keypass changeit
    keytool -certreq -alias "$identity" -keystore "certs/$identity.p12" -storepass changeit -file "certs/$identity.csr"
    # SAN is needed for broker hostname verification. Client certificates also get clientAuth.
    printf 'subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
' > "certs/$identity.ext"
    openssl x509 -req -in "certs/$identity.csr" -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -days 365 -extfile "certs/$identity.ext" -out "certs/$identity.crt"
    keytool -importcert -noprompt -alias ca -file certs/ca.crt -keystore "certs/$identity.p12" -storepass changeit
    keytool -importcert -alias "$identity" -file "certs/$identity.crt" -keystore "certs/$identity.p12" -storepass changeit
 done
# Docker's non-root broker only needs read access to these local lab files.
chmod a+r certs/*.p12 certs/*.crt
