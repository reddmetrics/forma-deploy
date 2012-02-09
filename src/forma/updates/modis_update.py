#! /usr/bin/env python

"""
This script updates our MODIS data archive stored on S3 based on what's
available on NASA's MODIS data FTP server (e4ftl01u.ecs.nasa.gov)

The MODIS FTP server is organized by product then date, in the format
/MODIS_Composites/MOLT/MOD13A3.005/2000-02-01. This corresponds to the Terra
(MOLT) vegetation indices product at 1000m resolution, 32-day composites,
for the month of February 2000.

This script gets a list of all date folders (e.g. 2000-02-01),
then checks whether we have any files in the corresponding date folder on S3
(e.g. s3://modisfiles/MOD13A3/2000-02-01/). If we use the quick and dirty
comparator (see  simpleS3FileCheck()), if there is even one file in this
directory the script will not download anything. If there are no files in
that directory, the script will download all files from the FTP server in
that date directory and upload it to the corresponding directory on S3.

A more comprehensive script could one day check whether all files for a given
date range have indeed been downloaded. See notes in exhaustiveS3FileCheck()

"""

import os
from datetime import date, timedelta
import traceback, ftplib
import sys

sys.path.append("../../..")

try:
    from forma.classes.ftp import FTPConn
    from forma.classes.file_obj import FileObj
    from forma.utils import utils
    from forma.config import static
except ImportError:
    from ftp import FTPConn
    from file_obj import FileObj
    import utils
    import static

import hipchat

#server = d.modis_ftp
#server = "e4ftl01u.ecs.nasa.gov"
#server = "e4ftl01.cr.usgs.gov" # as of September 13, 2011


class Results():
    error = dict()
    error["dates"] = dict()
    error["files"] = dict()
    success = dict()
    success["dates"] = dict()
    success["files"] = dict()
    get = dict()
    get["dates"] = dict()


def getFtpModisDatesList(server, ftp_base_path):
    # get a list of all the date directories on MODIS server
    # returns a list of the dates and an active FTPConn instance

    ftp = FTPConn(host=server, user="anonymous", passwd="anonymous@")

    print "\nGetting date directories on MODIS server"

    # first list element is size of directory - not useful
    dirlist = ftp.listdir(ftp_base_path, include_path=False)[1:]

    # only keep the name of the directory, which comes at the
    # end after a bunch of metadata

    dates = [dname[-10:] for dname in dirlist]

    return ftp, dates

def cleanDates(dates, filter):
    a, b, c = filter.split("-")
    date_filter = date(int(a), int(b), int(c))

    # Make sure dates are valid by casting as date types. This is really just
    # insurance against putting weird new files on our S3 account. Anything
    # that doesn't match the date pattern will be skipped (and logged as such)

    print "\nCleaning dates"

    outdates = []
    for modisdate in dates:

        try:
            # if the directories are named as we expect,
            # this should work just fine.
            yyyy, mm, dd = [int(i) for i in modisdate.split(".")]
            dt = date(int(yyyy), int(mm), int(dd))
            if dt >= date_filter:
                outdates.append(dt.isoformat())

        except:
            # if the split or date formatting doesn't work, or something else goes
            # wrong, we've got an unexpected directory structure on the server
            print "Invalid date:", modisdate

    return outdates

def genPaths(bucket, staging_bucket, modisfile, iso_date, product_prefix):
    """
    Generate paths for transfers based on date, modis filename, etc.
    Having this as a separate function makes testing easier.
    """

    product_prefix = product_prefix.split(".")[0] # drop .005 from raw prefix

    # e.g. ftp://e4ftl01u.ecs.nasa.gov/MODIS_Composites/MOLT/MOD13A3.005/2000.01.01/MOD13A3.A2000001.h08v06.005.2007111065956.hdf
    ftppath = "ftp://%s%s" % (static.modis_ftp(), modisfile)

    # e.g. /mnt/temp/2000-01-01/MOD13A3.A2000001.h08v06.005.2007111065956.hdf
    localpath = os.path.join(static.paths["temp"], iso_date, os.path.split(ftppath)[1])

    # e.g. /mnt/temp/2000-01-01/
    localbasepath = os.path.split(localpath)[0]

    # e.g. s3://modisfiles/MOD13A3/2000-01-01/MOD13A3.A2000001.h08v06.005.2007111065956.hdf
    s3path = "s3://%s/%s/%s/%s" % (bucket.name, product_prefix,
                                    iso_date, os.path.split(localpath)[1])

    s3staging = "s3://%s/%s/%s/%s" % (staging_bucket.name, product_prefix,
                                    iso_date, os.path.split(localpath)[1])

    return ftppath, localpath, localbasepath, s3path, s3staging

def checkModisFileOnS3(staging, bucket, staging_bucket, iso_date, product_prefix, modisfile):
    # only get actual hdf files, not jpg previews or xml metadata
    if modisfile[-4:] == ".hdf":

        ftppath, localpath, localbasepath, s3path, s3staging = genPaths(bucket,
                                                             staging_bucket,
                                                             modisfile,
                                                             iso_date,
                                                             product_prefix)
        if staging:
            if not utils.s3_exists(s3path, bucket) and not utils.s3_exists(s3staging, staging_bucket):
                return [ftppath, localpath, s3staging]
            else:
                return None
        else:
            if not utils.s3_exists(s3path, bucket):
                return [ftppath, localpath, s3path]
            else:
                return None

def getFtpFileList(ftp, ftp_base_path, modisdate, server=None):
    # have to go back to period seperators ...
    path = ftp_base_path + modisdate.replace("-", ".")

    print "Getting list of files for", modisdate

    try:
        # get a list of all files in this date directory
        filelist = ftp.list_nometa(path, include_path=False)
    except ftplib.error_temp:
        # timeout happens despite all the downloads because the
        # FileObj class actually stores an internal connection to the
        # FTP server. Thus no need to log back in. Because we're
        # using another connection to handle the directory crawling,
        # there's a timeout when you're downloading lots of stuff
        # (like 500m data). This wasn't an issue with 1000m data.

        print "FTP timeout - logging back in\n"
        ftp = FTPConn(host=server, user="anonymous", passwd="anonymous@")
        filelist = ftp.list_nometa(path, include_path=False)

    return ftp, filelist

def exhaustiveS3FileCheck(product_prefix, ftp, ftp_base_path, staging, bucket, staging_bucket, dates):
    """
    Instead of just checking whether any files at all have been uploaded to
    S3, we might want to know if all files of interest have been uploaded.
    """

    print "Checking whether we already have data for dates on server ..."

    to_get = {}
    m = 0
    n = 0

    for modisdate in dates:
        print "Checking", modisdate

        to_get[modisdate] = []

        ftp, filelist = getFtpFileList(ftp, ftp_base_path, modisdate)

        for modisfile in filelist:
            m += 1
            tile = utils.getModisTileFromFname(os.path.split(modisfile)[1])

            if tile in static.tiles:
                paths = checkModisFileOnS3(staging, bucket, staging_bucket, modisdate, product_prefix, modisfile)

                if paths:
                    to_get[modisdate].append(paths)
                    n += 1

    print "\nChecked %i files for %i dates\nAcquiring %i new file(s)" % (m, len(dates), n)
    return to_get, m

def getModisFiles(to_get, ftp):
    # paths is defined in checkModisFileOnS3()
    # s3path will be in the staging bucket or regular bucket,
    # depending on the results of checkModisFileOnS3()


    acquired = list()
    for date, paths in to_get.items():
        if paths:
            for ftp_local_s3 in paths:
                ftppath, localpath, s3path = ftp_local_s3

                local_base_path = os.path.split(localpath)[0]
                if not os.path.exists(local_base_path):
                    os.makedirs(local_base_path)

                f = FileObj()
                f.ftp = ftp
                if not os.path.exists(localpath):
                    f.get(ftppath, localpath)

                f.put(s3path, localpath, rrs=True)
                f.delete()
                acquired.append(s3path)
                
    return acquired

def sendUpdateStatusEmail(email, product_prefix, to_get, dates, checked, acquired):
    body = "%s data update status\n\n" % product_prefix
    body += "%i files checked\n" % checked
    body += "%i files acquired\n" % len(acquired)
    body += "\n%i date(s) checked:\n" % len(to_get.keys())
    body += "\n".join(dates)
    if acquired:
        body += "\nAcquired:\n"
        body += "\n".join(acquired)

    for address in email.split(" "):
        utils.sendStatusEmail(to_email=address, subject="[forma-data-update] %s: %s new files acquired" % (product_prefix, len(acquired)), body=body)
    return body

def parseCL():
    from optparse import OptionParser

    
    parser = OptionParser()
    parser.add_option("-r", "--resolution", default=None, help="Nominal resolution", dest="resolution")
    parser.add_option("-i", "--interval", default=None, help="Interval, in days, between datasets", dest="interval")
    parser.add_option("-f", "--filter", default=(date.today() - timedelta(90)).isoformat(), help="Filter out dates before filter string", dest="filter")
    parser.add_option("-b", "--bucket", default="modisfiles", help="S3 bucket to use for file checking", dest="bucket")
    parser.add_option("-a", "--staging-bucket", default="formastaging", help="S3 bucket to use for staging new files", dest="staging_bucket")
    parser.add_option("-g", "--staging", default=True, help="Upload missing files to staging bucket?", dest="staging")
    parser.add_option("-s", "--server", default="e4ftl01.cr.usgs.gov", help="MODIS FTP server", dest="server")
    parser.add_option("-k", "--kiss", default=True, help="Checking type - keep it simple, stupid or exhaustive", dest="kiss")
    parser.add_option("-p", "--product-prefix", default=None, help="MODIS product prefix", dest="product_prefix")
    parser.add_option("-e", "--email", default="rkraft4@gmail.com", help="Address(es) for status emails - space-separted string", dest="email")

    options, args = parser.parse_args()
    resolution = int(options.resolution)
    interval = int(options.interval)
    filter = options.filter
    bucket = options.bucket
    staging_bucket = options.staging_bucket
    staging = options.staging
    server = options.server
    kiss = options.kiss
    product_prefix = options.product_prefix
    email = options.email

    return bucket, staging_bucket, server, filter, product_prefix, staging, kiss, resolution, interval, email

def main(bucket="modisfiles", staging_bucket="formastaging", server=static.modis_ftp(), filter="2000-01-01",
         product_prefix=None, staging=True, kiss=False, resolution=None, interval=None, email="rkraft4@gmail.com"):

    # Settings
    if not product_prefix and resolution and interval:
        product_prefix = static.modis_products[resolution][interval]
    else:
        raise Exception("Must provide MODIS product prefix or resolution and interval")
    bucket = utils.s3_bucket_create(bucket_name=bucket)
    staging_bucket = utils.s3_bucket_create(bucket_name=staging_bucket)
    ftp_base_path = "/MODIS_Composites/MOLT/%s/" % product_prefix

    # do actual work
    ftp, dates = getFtpModisDatesList(server, ftp_base_path)
    dates = cleanDates(dates, filter)
    to_get, checked = exhaustiveS3FileCheck(product_prefix, ftp, ftp_base_path, staging, bucket, staging_bucket, dates)
    acquired = getModisFiles(to_get, ftp)

    # cleanup
    status = sendUpdateStatusEmail(email, product_prefix, to_get, dates, checked, acquired)
    hipchat.send_message(status)    
    
    return

if __name__ == "__main__":

    bucket, staging_bucket, server, filter, product_prefix, staging, kiss, resolution, interval, email = parseCL()
    main(bucket, staging_bucket, server, filter, product_prefix, staging, kiss, resolution, interval, email)

