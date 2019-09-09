"""
Craft a web request to the AWS rest API and hit an endpoint that actually works but isn't supported in the CLI or in Boto3.

Based on this: https://github.com/aws/aws-cli/issues/2268#issuecomment-373803942
"""
import boto3
import os
import sys, os, base64, datetime, hashlib, hmac, urllib
import requests

DEBUG = False

def get_database_region(db_instance_identifier):
    rds_client = boto3.client('rds')
    resp = rds_client.describe_db_instances(
        DBInstanceIdentifier=db_instance_identifier
    )
    region = resp['DBInstances'][0]['DBInstanceArn'].split(':')[3]
    return region

def get_credentials():
    session = boto3.Session()
    return session.get_credentials()

def get_log_file_via_rest(filename, db_instance_identifier):
    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def getSignatureKey(key, dateStamp, regionName, serviceName):
        kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
        kRegion = sign(kDate, regionName)
        kService = sign(kRegion, serviceName)
        kSigning = sign(kService, 'aws4_request')
        return kSigning

    # ************* REQUEST VALUES *************
    method = 'GET'
    service = 'rds'
    region = get_database_region(db_instance_identifier)
    host = 'rds.'+ region +'.amazonaws.com'
    endpoint = 'https://' + host

    # Key derivation functions. See:
    # http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
    credentials = get_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    session_token = credentials.token
    if access_key is None or secret_key is None:
        return 'No access key is available.'


    # Create a date for headers and the credential string
    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ') # Format date as YYYYMMDD'T'HHMMSS'Z'
    datestamp = t.strftime('%Y%m%d') # Date w/o time, used in credential scope

    # sample usage : '/v13/downloadCompleteLogFile/DBInstanceIdentifier/error/postgresql.log.2017-05-26-04'
    canonical_uri = '/v13/downloadCompleteLogFile/'+ db_instance_identifier + '/' + filename

    # Step 3: Create the canonical headers and signed headers. Header names
    # and value must be trimmed and lowercase, and sorted in ASCII order.
    # Note trailing \n in canonical_headers.
    # signed_headers is the list of headers that are being included
    # as part of the signing process. For requests that use query strings,
    # only "host" is included in the signed headers.
    canonical_headers = 'host:' + host + '\n'
    signed_headers = 'host'

    # Match the algorithm to the hashing algorithm you use, either SHA-1 or
    # SHA-256 (recommended)
    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = datestamp + '/' + region + '/' + service + '/' + 'aws4_request'

    # Step 4: Create the canonical query string. In this example, request
    # parameters are in the query string. Query string values must
    # be URL-encoded (space=%20). The parameters must be sorted by name.
    canonical_querystring = ''
    canonical_querystring += 'X-Amz-Algorithm=AWS4-HMAC-SHA256'
    canonical_querystring += '&X-Amz-Credential=' + urllib.parse.quote_plus(access_key + '/' + credential_scope)
    canonical_querystring += '&X-Amz-Date=' + amz_date
    canonical_querystring += '&X-Amz-Expires=30'
    if session_token is not None :
        canonical_querystring += '&X-Amz-Security-Token=' + urllib.parse.quote_plus(session_token)
    canonical_querystring += '&X-Amz-SignedHeaders=' + signed_headers

    # Step 5: Create payload hash. For GET requests, the payload is an
    # empty string ("").
    payload_hash = hashlib.sha256(''.encode("utf-8")).hexdigest()

    # Step 6: Combine elements to create create canonical request
    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

    # ************* TASK 2: CREATE THE STRING TO SIGN*************
    string_to_sign = algorithm + '\n' +  amz_date + '\n' +  credential_scope + '\n' +  hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()

    # ************* TASK 3: CALCULATE THE SIGNATURE *************
    # Create the signing key
    signing_key = getSignatureKey(secret_key, datestamp, region, service)

    # Sign the string_to_sign using the signing_key
    signature = hmac.new(signing_key, (string_to_sign).encode("utf-8"), hashlib.sha256).hexdigest()

    # ************* TASK 4: ADD SIGNING INFORMATION TO THE REQUEST *************
    # The auth information can be either in a query string
    # value or in a header named Authorization. This code shows how to put
    # everything into a query string.
    canonical_querystring += '&X-Amz-Signature=' + signature

    # ************* SEND THE REQUEST *************
    # The 'host' header is added automatically by the Python 'request' lib. But it
    # must exist as a header in the request.
    request_url = endpoint + canonical_uri + "?" + canonical_querystring

    if DEBUG:
        print('\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++')
        print('Request URL = ' + request_url)

    r = requests.get(request_url, allow_redirects=True)

    if DEBUG:
        print('\nRESPONSE++++++++++++++++++++++++++++++++++++')
        print('Response code: %d\n' % r.status_code)

    return r.text
