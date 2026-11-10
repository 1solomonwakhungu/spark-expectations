#!/bin/sh
# Generate a private development CA and a server certificate for the local stack.
set -eu
umask 077
output=${1:-containers/certs}
mkdir -p "$output"
for name in mailpit-ca.crt mailpit.crt mailpit.key; do
    if [ -e "$output/$name" ]; then
        echo "Refusing to overwrite existing development certificates in $output" >&2
        exit 1
    fi
done
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
cat > "$scratch/extensions.cnf" <<'CONFIG'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:mailpit,DNS:localhost,IP:127.0.0.1
CONFIG
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -keyout "$scratch/ca.key" -out "$scratch/ca.crt" \
    -subj '/CN=Local development mail CA' \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' 2>/dev/null
openssl req -new -newkey rsa:2048 -nodes -keyout "$scratch/server.key" \
    -out "$scratch/server.csr" -subj '/CN=mailpit' 2>/dev/null
openssl x509 -req -in "$scratch/server.csr" -CA "$scratch/ca.crt" \
    -CAkey "$scratch/ca.key" -CAcreateserial -days 30 \
    -extfile "$scratch/extensions.cnf" -out "$scratch/server.crt" 2>/dev/null
cp "$scratch/ca.crt" "$output/mailpit-ca.crt"
cp "$scratch/server.crt" "$output/mailpit.crt"
cp "$scratch/server.key" "$output/mailpit.key"
chmod 644 "$output/mailpit-ca.crt" "$output/mailpit.crt"
echo "Created development mail certificates in $output (valid for 30 days)."
