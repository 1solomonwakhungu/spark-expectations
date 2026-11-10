#!/bin/sh
# Generate a private development CA and a server certificate for the local stack.
set -eu
umask 077
# Python provides exact-path hard links on both macOS and Linux.
command -v python3 >/dev/null
output=${1:-containers/certs}
if [ ! -d "$output" ]; then
    mkdir -p "$output"
    chmod 755 "$output"
fi
for name in mailpit-ca.crt mailpit.crt mailpit.key; do
    if [ -e "$output/$name" ] || [ -L "$output/$name" ]; then
        echo "Refusing to overwrite existing development certificates in $output" >&2
        exit 1
    fi
done
scratch=$(mktemp -d "$output/.mail-certs.XXXXXX")
published=""
completed=false
cleanup() {
    if [ "$completed" = false ]; then
        for name in $published; do rm -f "$output/$name"; done
    fi
    rm -rf "$scratch"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
cat > "$scratch/extensions.cnf" <<'CONFIG'
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:mailpit,DNS:localhost,IP:127.0.0.1
CONFIG
# OpenSSL may reject long absolute CA paths when deriving its serial filename.
# Keep generation paths short without changing the staging or publication paths.
(
    cd "$scratch"
    openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
        -keyout ca.key -out ca.crt \
        -subj '/CN=Local development mail CA' \
        -addext 'basicConstraints=critical,CA:TRUE' \
        -addext 'keyUsage=critical,keyCertSign,cRLSign' \
        -addext 'subjectKeyIdentifier=hash' \
        -addext 'authorityKeyIdentifier=keyid:always' 2>/dev/null
    openssl req -new -newkey rsa:2048 -nodes -keyout server.key \
        -out server.csr -subj '/CN=mailpit' 2>/dev/null
    openssl x509 -req -in server.csr -CA ca.crt \
        -CAkey ca.key -CAcreateserial -days 30 \
        -extfile extensions.cnf -out server.crt 2>/dev/null
)
# Exact-path hard links refuse existing entries, including directories and symlinks.
# Staging under the destination keeps the files on the same filesystem.
link_exact() {
    python3 -c 'import os, sys; os.link(sys.argv[1], sys.argv[2])' "$1" "$2"
}
link_exact "$scratch/ca.crt" "$output/mailpit-ca.crt"
published="mailpit-ca.crt"
link_exact "$scratch/server.crt" "$output/mailpit.crt"
published="$published mailpit.crt"
link_exact "$scratch/server.key" "$output/mailpit.key"
published="$published mailpit.key"
chmod 644 "$output/mailpit-ca.crt" "$output/mailpit.crt"
completed=true
echo "Created development mail certificates in $output (valid for 30 days)."
