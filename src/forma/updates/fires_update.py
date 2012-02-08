#! /usr/bin/env python

"""
All we're doing here is getting a list of all the files on the UMD
server, which is updated every day to keep only the most recent ~60 days
of data available.

Once we have the list, check whether we're missing any
of those files (except for the last one, which is most recent and
possibly incomplete for today).

After checking whether we have a given file, if it's missing download it from
the FTP server and upload it to S3.
"""

import os
import sys

sys.path.append("../../../")
sys.path.append("../../")

try:
    from forma.classes.file_obj import FileObj
    from forma.classes.ftp import FTPConn
    from forma.utils import utils
    from forma.config import static

except ImportError:
    from file_obj import FileObj
    from ftp import FTPConn
    import utils
    import static


#firespath = "fires/global/after_day60_2010/" # booooo globals!

def checkFiresFilesOnS3(server, fireslist, bucket, staging_bucket):

    # skip the last 2 files in fireslist - last one is from today,
    # probably incomplete, and second to last one is from yesterday and may
    # not yet be finalized

    to_get = []
    for fname in fireslist[:-2]:
        print "\nProcessing", fname

        # setup for checking if we've already got a particular file
        ftppath = "ftp://%s%s" % (server, fname)
        s3path = "s3://%s/DailyFires/%s" % (bucket.name, os.path.split(ftppath)[1])
        staging_path = "s3://%s/DailyFires/%s" % (staging_bucket.name, os.path.split(ftppath)[1])


        if not utils.s3_exists(staging_path, staging_bucket) and not utils.s3_exists(s3path, bucket):
            localpath = static.paths["temp"] + os.path.split(fname)[1] # drop /Global/ from pathname
            to_get.append([ftppath, staging_path, localpath])
        else:
            print "\nAlready have this file"
            print ftppath

    return to_get

def parseCL():
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-b", "--bucket", default="modisfiles", help="S3 bucket to use for file checking", dest="bucket")
    parser.add_option("-a", "--staging-bucket", default="formastaging", help="S3 bucket to use for staging new files", dest="staging_bucket")
    parser.add_option("-g", "--staging", default=True, help="Upload missing files to staging bucket", dest="staging")
    parser.add_option("-e", "--email", default="rkraft4@gmail.com", help="Address(es) for status email - space-separated string", dest="email")

    options, args = parser.parse_args()

    bucket = options.bucket
    staging_bucket = options.staging_bucket
    staging = options.staging
    email = options.email

    return staging, bucket, staging_bucket, email

def main(staging=False, bucket="modisfiles", staging_bucket="formastaging", email="rkraft4@gmail.com"):

    server, username, password = static.getFiresParams()

    # we're putting everything into a new bucket
    staging_bucket = utils.s3_bucket_create(staging_bucket)
    bucket = utils.s3_bucket_create(bucket)

    
    # Get list of fires files
    ftp = FTPConn(server, user=username, passwd=password)
    fireslist = ftp.list_nometa("/Global/")

    # Check whether each file has been downloaded already
    to_get = checkFiresFilesOnS3(server, fireslist, bucket, staging_bucket)

    f = FileObj()

    acquired = list()
    for ftppath, s3path, localpath in to_get:

        # Get the file from UMD FTP server
        print "Getting %s\n" % ftppath
        f.get(ftppath, localpath, user=username, password=password)
        f.put(s3path, localpath)
        f.delete()
        acquired.append(s3path)


    body = "Fires data update status\n\n"
    body += "%i files checked\n" % len(fireslist)
    body += "%i files acquired\n" % len(acquired)
    if acquired:
        body += "\n".join(acquired)

    for address in email.split(" "):
        utils.sendStatusEmail(to_email=address, subject="[forma-data-update] %s: %s new files acquired" % ("Fires", len(acquired)), body=body)
    return

if __name__ == "__main__":

    staging, bucket, staging_bucket, email = parseCL()
    main(staging, bucket, staging_bucket)
