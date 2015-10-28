#!/usr/bin/env python
"""
A script to assist in uploading large files to S3.

Uses some code from documentation in boto3.s3.transfer pretty much verbatim
(ProgressPercentage class, upload retry loop)
"""
#pylint: disable=C0103

import argparse
import logging
import os
import re
import sys
import threading
import time
import urlparse

import boto3
from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.exceptions import ClientError

LOG = logging.getLogger("s3-mp-upload")

UNIT_MULTIPLIERS = {
    "byte": 1,
    "kilobyte": 1000,
    "kB": 1000,
    "kibibyte": 1024,
    "KiB": 1024,
    "KB": 1024,
    "megabyte": 1000**2,
    "MB": 1000**2,
    "mebibyte": 1024**2,
    "MiB": 1024**2,
    "gigabyte": 1000**3,
    "GB": 1000**3,
    "gibibyte": 1024**3,
    "GiB": 1024**3,
    "terabyte": 1000**4,
    "TB": 1000**4,
    "tebibyte": 1024**4,
    "TiB": 1024**4,
    "petabyte": 1000**5,
    "PB": 1000**5,
    "pebibyte": 1024**5,
    "PiB": 1024**5,
    "exabyte": 1000**6,
    "EB": 1000**6,
    "exbibyte": 1024**6,
    "EiB": 1024**6,
    "zettabyte": 1000**7,
    "ZB": 1000**7,
    "zebibyte": 1024**7,
    "ZiB": 1024**7,
    "yottabyte": 1000**8,
    "YB": 1000**8,
    "yobibyte": 1024**8,
    "YiB": 1024**8,
}

DEFAULTS = {
    "p_transfers": 2,
    "max_tries": 10,
    "split": "50 MiB",
    "chunk_threshold": "50 MiB",
    # Wait 10 seconds before initial retry, if there was an error during upload.
    # NOTE: This value will double with each failure
    "retry_sleep": 10,
}

def chunk_bytes(chunk_string, default_unit="byte", default_multiplier="MiB"):
    """
    Convert a string to the total bytes that it represents

    :param str chunk_string: A string describing a multiple of bytes to return.
    :param str default_unit: The default unit to use (bytes, of course)
    :param str default_multiplier: The multiplier to use, if chunk_string is just a digit.
    :return The total bytes represented by the string
    :rtype int
    :raises ValueError: If the string's fields look bad (count or unit multiplier)
    :raises RuntimeError: If the string was not parseable.
    """

    if chunk_string.isdigit():
        # Assume the default_unit (byte) and multiplier (MiB)
        return UNIT_MULTIPLIERS[default_unit] * (UNIT_MULTIPLIERS[default_multiplier] * int(chunk_string))

    if "." in chunk_string:
        error_message = "Split string must use whole numbers!: {0}".format(chunk_string)
        LOG.error(error_message)
        raise ValueError(error_message)

    result = re.match(r'^(\d+)\s*(\S+)?', chunk_string, flags=re.UNICODE)

    if not result:
        error_message = "Could not parse split string: {0}!".format(chunk_string)
        LOG.error(error_message)
        raise RuntimeError(error_message)

    count_s, multiplier = result.groups()
    count = int(count_s)

    if multiplier is None:
        # chunk_string has trailing whitespace and no unit
        multiplier = default_multiplier

    if multiplier.lower().endswith("bytes"):
        # Chop off the ending 's' of '<something>bytes', if necessary
        multiplier = multiplier[:-1]

    matching_multiplier = None
    if multiplier in UNIT_MULTIPLIERS:
        matching_multiplier = multiplier
    else:
        for mp_name in UNIT_MULTIPLIERS:
            if multiplier.lower() == mp_name.lower():
                matching_multiplier = mp_name
                break

    if not matching_multiplier:
        error_message = "Invalid units specified in split string: {0}!".format(chunk_string)
        LOG.error(error_message)
        raise ValueError(error_message)

    return UNIT_MULTIPLIERS[default_unit] * (UNIT_MULTIPLIERS[matching_multiplier] * count)

# pylint: disable=C0111,R0903
class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far,
                                             self._size, percentage))
            sys.stdout.flush()

#pylint: disable=R0914,W0613
def main(src, dest, num_processes=DEFAULTS["p_transfers"], force=False,
         chunk=DEFAULTS["split"], reduced_redundancy=False, secure=True,
         max_tries=DEFAULTS["max_tries"], verbose=False, quiet=False,
         retry_sleep=DEFAULTS["retry_sleep"]):
    """
    Send a file to S3, potentially in parallel chunked transfers.

    :param file src: File-like object that supports f.name attribute, to use as source of transfer.
    :param str dest: The destination s3 object uri, to transfer the source file to.
    (This is filled-in with the source filename if the destination uri is an incomplete object path)
    :param int num_processes: Number of parallel transfers.
    :param bool force: Set to True if overwriting destination s3 object is ok.
    :param str chunk: A string describing the chunk size to use.
    :param bool reduced_redundancy: Unused parameter for compatibility purposes.
    :param bool secure: Unused parameter for compatibility purposes.
    :param bool verbose: Set to true, for more output.
    :param bool quiet: Set to true, for less output.
    :param int retry_sleep: The number of seconds to sleep between upload attempts, if there is a failure.
    (This value is doubled with each failure)
    :return None
    :rtype None
    """
    # Determine the size of chunks, based on chunk-string multiplier
    chunk_size = chunk_bytes(chunk)

    # Get S3 path
    dest_uri_parts = urlparse.urlsplit(dest)
    if dest_uri_parts.scheme != "s3":
        error_message = "Destination needs to be an S3 url!: {0}".format(dest)
        LOG.error(error_message)
        raise ValueError(error_message)

    s3_bucket = dest_uri_parts.netloc
    s3_dest_path = dest_uri_parts.path

    client = boto3.client("s3", use_ssl=True)

    # Check that bucket exists
    try:
        client.head_bucket(Bucket=s3_bucket)
    except ClientError as e:
        LOG.error("Could not inspect s3 bucket %s!", s3_bucket)
        raise

    # Generate a destination object name, if necessary
    if not s3_dest_path or s3_dest_path == "/":
        # Destination is bucket root
        source_file = os.path.split(src.name)[1]
        s3_dest_obj = source_file
    elif s3_dest_path.endswith("/"):
        # Destination is partial-path, not object
        source_file = os.path.split(src.name)[1]
        s3_dest_obj = os.path.join(s3_dest_path, source_file)
    else:
        # Full path (always strip leading /)
        s3_dest_obj = s3_dest_path.lstrip("/")

    # Check if destination object exists
    try:
        client.get_object(Bucket=s3_bucket, Key=s3_dest_obj)
    except ClientError as e:
        if "NoSuchKey" in str(e):
            # It's ok if the object doesn't exist; We'll create it.
            pass
        else:
            LOG.error("Error checking for destination object!")
            raise
    else:
        # Key exists
        if not force:
            error_message = "{0} alredy exists in {1}. Use --force to overwrite!".format(s3_dest_obj, s3_bucket)
            LOG.error(error_message)
            raise RuntimeError(error_message)

    # Transfer config
    config = TransferConfig(
        multipart_threshold=chunk_bytes(DEFAULTS["chunk_threshold"]),
        multipart_chunksize=chunk_size,
        max_concurrency=num_processes,
        # NOTE: boto3.transfer.upload_file() doesn't currently have an equivalent of num_download_attempts.
        # We'll implement this ourselves, for now.
        #num_download_attempts=max_tries,
    )

    # Start transfer
    LOG.info("Starting upload")
    transfer = S3Transfer(client, config)
    callback_fn = None
    if verbose:
        callback_fn = ProgressPercentage(src.name)

    retry_interval = retry_sleep
    last_exception = RuntimeError("Upload and retries failed, but without raising an exception?!")
    #pylint: disable=W0612
    for attempt in range(max_tries):
        #pylint: disable=W0703
        try:
            transfer.upload_file(src.name, s3_bucket, s3_dest_obj, callback=callback_fn)
        except Exception as e:
            LOG.error("Error during upload!", exc_info=True)
            last_exception = e
            if verbose:
                # Reset upload counter
                callback_fn = ProgressPercentage(src.name)
        else:
            sys.stdout.write("\r\n")
            sys.stdout.flush()
            LOG.info("Finished upload")
            return
        LOG.warning("Sleeping for %d seconds before next attempt", retry_interval)
        time.sleep(retry_interval)
        retry_interval *= 2
    LOG.error("Maximum upload attempts exceeded!")
    raise last_exception

if __name__ == "__main__":
    LOG.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    LOG.addHandler(ch)

    parser = argparse.ArgumentParser(
        description="Upload large files to S3 using parallel chunked transfers",)

    parser.add_argument("src",
                        type=file,
                        help="Source file to transfer",)

    parser.add_argument("dest",
                        help="Destination S3 object of transfer",)

    parser.add_argument("-np", "--num-processes",
                        type=int,
                        default=DEFAULTS["p_transfers"],
                        help="Number of parallel transfers (default: {0})".format(DEFAULTS["p_transfers"]),)

    parser.add_argument("-f", "--force",
                        action="store_true",
                        help="Overwrite any pre-existing S3 object with the same name",)

    parser.add_argument("-s", "--split",
                        dest="chunk",
                        default=DEFAULTS["split"],
                        help="Split size to use (default: {0}). Accepts suffixes: {1}".format(
                            DEFAULTS["split"], ", ".join(sorted(UNIT_MULTIPLIERS.keys()))),)

    parser.add_argument("--rrs", "--reduced-redundancy",
                        dest="reduced_redundancy",
                        action="store_true",
                        help="Unused parameter, here for compatibility purposes",)

    parser.add_argument("--insecure",
                        dest="secure",
                        action="store_false",
                        help="Unused parameter, here for compatibility purposes",)

    parser.add_argument("-t", "--max-tries",
                        type=int,
                        default=DEFAULTS["max_tries"],
                        help="Maximum upload attempts, before failure (default: {0})".format(DEFAULTS["max_tries"]),)

    parser.add_argument("-v", "--verbose",
                        action="store_true",
                        help="Print more output",)

    parser.add_argument("-q", "--quiet",
                        action="store_true",
                        help="Print less output",)

    args = parser.parse_args()
    if args.quiet:
        LOG.setLevel(logging.WARNING)
    if args.verbose:
        LOG.setLevel(logging.DEBUG)

    args_dict = vars(args)

    LOG.debug("Args: %s", args_dict)
    main(**args_dict)
