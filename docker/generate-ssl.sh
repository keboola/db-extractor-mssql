#!/bin/bash
set -e

# FROM: https://gist.github.com/komuw/076231fd9b10bb73e40f
export TARGET_DIR="certificates"
export DAYS=50000
export SSL_HOST="mssql-ssl"

# Cleanup
rm -rf $TARGET_DIR
mkdir $TARGET_DIR
cd $TARGET_DIR

########################################################
################## CREATE CERTIFICATES #################
########################################################

# Create the CA Key and Certificate for signing Client Certs
openssl genrsa -out ca.key 4096
openssl req -subj "/CN=invalidCNCa" -new -x509 -days $DAYS -key ca.key -out ca.crt

# Create the Server Key, CSR, and Certificate
openssl genrsa -out mssql.key 4096
openssl req -subj "/CN=${SSL_HOST}" -new -key mssql.key -out mssql.csr

# We're self signing our own server cert here.  This is a no-no in production.
openssl x509 -req -days $DAYS -in mssql.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out mssql.crt

# Create the Client Key and CSR
openssl genrsa -out client.key 4096
openssl req -subj "/CN=-client" -new -key client.key -out client.csr

# Sign the client certificate with our CA cert.  Unlike signing our own server cert, this is what we want to do.
# Serial should be different from the server one, otherwise curl will return NSS error -8054
openssl x509 -req -days $DAYS -in client.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out client.crt

# Verify Server Certificate
openssl verify -purpose sslserver -CAfile ca.crt mssql.crt

# Verify Client Certificate
openssl verify -purpose sslclient -CAfile ca.crt client.crt


########################################################
################ CREATE INVALID CN CERT ################
########################################################

# Create the CA Key and Certificate for signing Client Certs
openssl genrsa -out invalidCNCa.key 4096
openssl req -subj "/CN=invalidCa" -new -x509 -days $DAYS -key invalidCNCa.key -out invalidCNCa.crt

# Create the Server Key, CSR, and Certificate
openssl genrsa -out mssql-invalidCn.key 4096
openssl req -subj "/CN=${SSL_HOST}-invalidCn" -new -key mssql-invalidCn.key -out mssql-invalidCn.csr

# We're self signing our own server cert here.  This is a no-no in production.
openssl x509 -req -days $DAYS -in mssql-invalidCn.csr -CA invalidCNCa.crt -CAkey invalidCNCa.key -set_serial 01 -out mssql-invalidCn.crt

# Create the Client Key and CSR
openssl genrsa -out client-invalidCn.key 4096
openssl req -subj "/CN=-client-invalidCn" -new -key client-invalidCn.key -out client-invalidCn.csr

# Sign the client certificate with our CA cert.  Unlike signing our own server cert, this is what we want to do.
# Serial should be different from the server one, otherwise curl will return NSS error -8054
openssl x509 -req -days $DAYS -in client-invalidCn.csr -CA invalidCNCa.crt -CAkey invalidCNCa.key -set_serial 02 -out client-invalidCn.crt

# Verify Server Certificate
openssl verify -purpose sslserver -CAfile invalidCNCa.crt mssql-invalidCn.crt

# Verify Client Certificate
openssl verify -purpose sslclient -CAfile invalidCNCa.crt client-invalidCn.crt

