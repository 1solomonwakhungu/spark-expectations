#!/bin/sh
# Validate a release version before it is used to publish documentation.
#
# Accepted grammar: an optional lowercase "v" followed by MAJOR.MINOR.PATCH,
# where each part is a decimal number without leading zeros, for example
# "2.10.1" (what `hatch version` reports on a release tag) or "v2.10.1"
# (the format documented for the manual docs workflow).
#
# Usage: validate_release_version.sh VERSION
# On success the version is printed unchanged and the exit status is 0.
# On failure a message is written to stderr and the exit status is non-zero.
# The value is only ever handled as data: it is never evaluated by the shell.

set -eu
LC_ALL=C
export LC_ALL

if [ "$#" -ne 1 ]; then
    echo "usage: $0 VERSION" >&2
    exit 2
fi

value=$1

reject() {
    echo "error: invalid release version; expected MAJOR.MINOR.PATCH with an optional leading 'v' (for example v2.10.1)" >&2
    exit 1
}

# Bound the length so the checks below never see unbounded input.
if [ "${#value}" -eq 0 ] || [ "${#value}" -gt 32 ]; then
    reject
fi

# grep matches line by line, so reject embedded newlines and carriage returns
# explicitly before using it.
newline='
'
carriage_return=$(printf '\r')
case $value in
    *"$newline"* | *"$carriage_return"*) reject ;;
esac

if ! printf '%s\n' "$value" | grep -Eq '^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'; then
    reject
fi

printf '%s\n' "$value"
