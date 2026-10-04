# Demonstration keys and certificates

The private keys in this directory are public. They are checked in so that
the demos in [`examples/`](../../README.md) run from a clean clone with
nothing to generate first, they protect nothing, and anyone can read them.
**Use them for the demos only, never in production.**

To make your own, with openssl 3 (these are the commands the demo's files
were made with; `-copy_extensions` carries the `subjectAltName` into the
signed certificate):

```sh
# One authority. It signs both the broker and the device.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -days 825 -nodes -subj "/CN=my-ca" \
  -keyout ca-key.pem -out ca.pem

# The broker's certificate. The subjectAltName must say what clients dial.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=broker.example.com" \
  -addext "subjectAltName=DNS:broker.example.com" \
  -keyout broker-key.pem -out broker.csr
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 825 -copy_extensions copy -out broker.pem

# A device. The Common Name becomes the client's user name.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=device-7" -keyout device-7-key.pem -out device-7.csr
openssl x509 -req -in device-7.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 825 -out device-7.pem

rm -f broker.csr device-7.csr ca.srl
```

Keep the new `*-key.pem` files out of version control and readable only by
the account that runs the broker (`chmod 600`). The
[examples README](../../README.md) explains what each file is for.
