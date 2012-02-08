import os, subprocess, sys
from time import sleep
sys.path.append("../")
import numpy as np

try:
    from forma.config import static
except ImportError:
    import static

from boto.s3.connection import S3Connection
from boto.exception import S3CreateError
from boto.ses.connection import SESConnection


try:
    from matplotlib.mlab import rec_groupby
except ImportError:
    print "FYI Matplotlib must be installed to use the collapse function.\nOther functions should work as expected."


def dostata(dofile, verbose=True, *args):
    '''All arguments must be strings; the numerical arguments
    should be transformed using the str(arg) function.  Note
    that Stata has to be called from the command line using
    argument "stata".'''

    cmd = ["stata", "do", dofile]
    for arg in args:
        cmd.append(arg)

    if verbose: print "\nDo-file: " + dofile
    retcode = subprocess.call(cmd)
    if verbose: print "Completed! For better or for worse. Stata closed."

    return retcode


def transfer(localPath, remotePath, direction=None, force=True, progress=False, verbose=True):
    '''Transfer files between EC2 and S3.  Capacity to download an entire directory.

    localPath   --str   local path of either target or source file
    remotePath  --str   remote path on S3, including the prefix and bucket (e.g., s3://forma_test/)
    direction   --str2  specifies download "dl" from S3 or upload "ul" to S3
    force       --bool  overwrite file in upload
    verbose     --bool  boolean for print statements
    '''

    '''Base function in order to append additional arguments and parameters'''
    cmd = ["s3cmd", direction]

    if force: cmd.insert(2,"--force")
    if os.path.isdir(localPath): cmd.insert(2, "-r")
    if not progress:  cmd.insert(2,"--no-progress")

    if direction == "put":
        cmd.extend([localPath, remotePath])
        printDirection = "Upload"
    else:
        cmd.extend([remotePath, localPath])
        printDirection = "Download"

    if verbose: print "\nTransfer direction: %s \
                       \nLocal file: %s \
                       \nRemote file: %s \
                       \nCommand: %s" % (printDirection, localPath, remotePath, cmd)

    retcode = subprocess.call(cmd)
    return retcode

def get(remote, local=None, force=True, progress=False, verbose=True):
    if not local:
        local = os.path.split(remote)[1]
    retcode = transfer(localPath=local, remotePath=remote, direction="get", force=True, progress=False, verbose=True)
    return retcode

def put(local, remote, force=True, progress=False, verbose=True):
    retcode = transfer(localPath=local, remotePath=remote, direction="put", progress=False, verbose=True)

    return retcode

def gunzip(filePath, force=True):
    # G-unzip a file, specified by full path.
    "\nUncompressing %s ..." %filePath
    cmd = ['gunzip', filePath]
    if force: cmd.append('--force')
    subprocess.call(cmd)
    "... uncompression completed."


def ls2stata(pyList):
    # Convert a python list to a list that can be passed to Stata via command line
    stlist = '"' + " ".join(pyList) + '"'
    return stlist

def genAsciiHeader(nrows=180, ncols=360, xll=-180, yll=-90,
                   cellsize=1, nodataval=-999):
    header = str(
                 "ncols        %20.0f\n" % ncols +
                 "nrows        %20.0f\n" % nrows +
                 "xllcorner    %20.8f\n" % xll +
                 "yllcorner    %20.8f\n" % yll +
                 "cellsize     %20.8f\n" % cellsize +
                 "NODATA_value %20i"     % nodataval +
                 "\n")
    return header

def collapse(arr, group_by, collapse_fields, functions, out_fields=None):
    """Provides some functionality for recarrays similar to Stata's collapse command

    Ex.: collapse (mean) weight, by(state)
    collapse(rec_array, ['state'], ['weight'], [np.mean])

    Takes a record array.
    group_by: list of field names by which to summarize
    collapse_fields: list of fields to summarize
    functions: list of numpy functions, like np.mean, np.median, np.sum (no parens)
    out_fields: names of fields to be generated; if None, collapses collapse_fields in place
    """
    if not out_fields:
        out_fields = [i for i in collapse_fields]
    if len(collapse_fields) != len(out_fields):
        raise ValueError("Number of collapse variables must equal number of outvars")
    stats = zip(collapse_fields, functions, out_fields)
    return rec_groupby(arr, group_by, stats)

def sample(baseCoords, raster, rastdesc, noData=None):

    # TODO: tweak function to take a tuple of coordinates, for better compatibility with shapely.

    #### Still need to deal with input points that are outside of raster extent

    # The baseCoords is a grid with x-coordinates in the first column, y-coords
    # in the second column. The "raster" is an ASCII grid, with the prelude
    # lopped off.  The "noData" parameter specifies the value assigned to
    # points in the base coordinate grid outside of the raster's extent.

    print "sampling..."

    # Be sure that the list matches the order where "rastdesc" is first
    # defined.

    left, right, bottom, top, nrows, ncols, cellsize = rastdesc
    numGridPoints = baseCoords.shape[0]

    # The numerator adjusts the origin to the bottom, left of the extent;
    # The negative signs on "left" and "bottom" (in note) assume that the
    # lower left corner is something like (-180, -90). Note that the
    # numerator on yi is a simplified version of the equation:
    # ((top - bottom) - (y - bottom)) = (top - y)

    xi = np.ceil((baseCoords[:,0] - left)/float(cellsize)).astype(int)
    yi = np.ceil((top - baseCoords[:,1])/float(cellsize)).astype(int)

    # Create an array to mask out all grid points that are outside of the
    # raster extent.  In this case, the mask is applied to positive values,
    # so that "0" *is not* masked out, while "1" *is* masked out.

    mask = np.zeros(xi.shape)
    mask[np.where(xi > ncols)] = 1
    mask[np.where(yi > nrows)] = 1

    # Get an array of all of the values that are still unmasked.

    ids = ma.masked_array(np.arange(xi.shape[0]), mask)
    ids = ids[~ids.mask].compressed()

    xii = ma.masked_array(xi, mask)
    xii = xii[~xii.mask].compressed()

    yii = ma.masked_array(yi, mask)
    yii = yii[~yii.mask].compressed()

    # The adjusted y-values indicate the rows; the adjusted x-values
    # indicate the columns, noting that the indexing starts at "0".

    val = raster[(yii-1), (xii-1)].astype(int)
    fval = np.repeat(noData, numGridPoints)
    fval[ids] = val

    print "...sample complete."
    return fval

def getRasterDesc(rasterDict):
    """Get raster description in the format needed for the sample function.
    Basically unpack the raster dictionary."""

    left     = rasterDict['left']
    bottom   = rasterDict['bottom']
    cellsize = rasterDict['cellsize']
    ncols    = rasterDict['ncols']
    nrows    = rasterDict['nrows']

    right    = left + (ncols * cellsize)
    top      = bottom + (nrows * cellsize)

    return left, right, bottom, top, nrows, ncols, cellsize

def createGrid(extent, cellsize):
    left, right, bottom, top = extent
    x = np.arange(left, right + cellsize, cellsize)
    y = np.arange(bottom, top + cellsize, cellsize)
    xx, yy = np.meshgrid(x, y)
    xx = xx.reshape((-1,1))
    yy = yy.reshape((-1,1))
    return np.hstack((xx,yy))

def ascii2numpy(fullPath, finalDir, overwrite=False):
    npyFile = getFileName(fullPath, extension=False) + ".npy"
    print "\nSaving: ", npyFile
    if os.path.exists(os.path.join(finalDir, npyFile)):
        print "numpy file already exists: ", os.path.join(finalDir, npyFile)
    else:
        raster = np.loadtxt(fullPath, skiprows=6)
        np.save(os.path.join(finalDir, npyFile), raster)

def loadAsciiHeader(f):
    """Read first six lines of ASCII grid to get header info"""

    # TODO: something to assign values using getattr()?
    #lines = [f.readline() for i in xrange(6)]

    ncols     = int(f.readline().strip().split("ncols")[1].strip())
    nrows     = int(f.readline().strip().split("nrows")[1].strip())
    xll       = float(f.readline().strip().split("xllcorner")[1].strip())
    yll       = float(f.readline().strip().split("yllcorner")[1].strip())
    cellsize  = float(f.readline().strip().split("cellsize")[1].strip())
    nodataval = int(f.readline().strip().split("NODATA_value")[1].strip())

    header = genAsciiHeader(nrows, ncols, xll, yll, cellsize, nodataval)
    header_list = [nrows, ncols, xll, yll, cellsize, nodataval]

    return header, header_list

def resampleCheckDimensions(arr, window):
    # Make sure dimensions are ok for resample window
    if type(window) != tuple:
        print "Resampling window must be a tuple, e.g. (2, 2)"
        return False

    h = int(arr.shape[0] / float(window[0]))
    v = int(arr.shape[1] / float(window[1]))

    # this means this didn't divide evenly in the lines above
    if h != int(h) or v != int(v):
        print "Array dimensions %s are invalid for resample window %s." % (
                                                    str(arr.shape), str(window)
                                                                          )
        return False

    else:
        return True

def resample(arr, resample_function, window):

    #===========================================================================#
    # Elegant resampling as suggested by Pauli on the Numpy mailing list (thx!) #
    # http://mail.scipy.org/pipermail/numpy-discussion/2010-July/051760.html    #
    # Default is a 2 x 2 window                                                 #
    #===========================================================================#

    # be sure the resampling window
    if not resampleCheckDimensions(arr, window):
        print "Resample failed. Check dimensions of array and resample window"
        print "Returing original array"
        return arr

    rs_x = window[0]
    rs_y = window[1]
    nrows = arr.shape[0]
    ncols = arr.shape[1]

    arr = arr.view().reshape(nrows/rs_x, rs_x, ncols/rs_y, rs_y)


    if resample_function == "sum":
        return arr.view().sum(axis=3).sum(axis=1)
    elif resample_function == "mean":
        return arr.view().mean(axis=3).mean(axis=1)
    elif resample_function == "min":
        return arr.view().min(axis=3).min(axis=1)
    elif resample_function == "max":
        return arr.view().max(axis=3).max(axis=1)
    else:
        print "Resampling method %s not recognized" % str(resample_function)
        print "Resample failed"
        return arr

def cl_modis_product_parser(argv):

    modis_products = {1000:{32:"MOD13A3.005", 16:"MOD13A2.005"},
                      500:{16:"MOD13A1.005"},
                      250:{16:"MOD13Q1.005"}}

    if len(argv) == 2:
        # pass the product prefix alone and you're good to go
        res = int(argv[1])
        if res == 500 or res == 250:
            interval = 16
        else:
            interval = 32 # default
        product_prefix = modis_products[res][interval]

    elif len(argv) == 3:
        res = int(argv[1])
        interval = int(argv[2])

        product_prefix = modis_products[res][interval]

    else:
        # defaults to 1000m 32-day
        product_prefix = "MOD13A3.005"
        res = 1000
        interval = 32

    return product_prefix, res, interval

def getModisTileFromFname(fname):
    return fname[18:20] + fname[21:23]

def getAwsConfig():
    #    import ConfigParser
    #    config = ConfigParser.RawConfigParser()
    #    config.read('../config/ec2config.cfg')
    #    aws_id = config.get("aws info", "aws_access_key_id")
    #    aws_secret = config.get("aws info", "aws_secret_access_key")

    return static.aws_access_key_id, static.aws_secret_access_key

def s3_bucket_create(bucket_name):
    """Instantiates boto's S3 bucket class using an existing S3 connection class instance,
    but avoids total failure due to simultaneous instantiation across python instances by
    retrying the .create_bucket() method 30 times with a 1 second delay.

    In the unlikely event that these collisions occur 30 times in a row over a 30 second
    period, the entire script aborts. You should look at the S3 system status page in such cases:
    http://status.aws.amazon.com/"
    """
    print "Creating bucket connection:", bucket_name
    bucket_conn = None
    bucket_tries = 0
    max_tries = 30

    aws_id, aws_secret = getAwsConfig()
    s3 = S3Connection(aws_id, aws_secret)

    while not bucket_conn or bucket_tries <= max_tries:
        try:
            bucket_conn = s3.create_bucket(bucket_name)
            return bucket_conn
        except S3CreateError:
            bucket_tries += 1
            print "Bucket create error. Retrying."
            sleep(bucket_tries)
        if bucket_tries == max_tries:
            raise Exception("Tried and failed to create bucket connection to [%s] %i times." % (bucket_name, bucket_tries))
    return bucket_conn

def s3_exists(remote, bucket_conn=None, return_bucket=False):
    """
    Check whether a particular file already exists on S3

    Requires converting path for s3cmd to format appropriate for boto
    e.g. 's3://forma_test/data/temp/modis/raw/1000/MOD13A3.A2000032.h33v09.005.2006271173933.hdf'
    becomes
    'data/temp/modis/raw/1000/MOD13A3.A2000032.h33v09.005.2006271173933.hdf'
    """
    bucket_name = remote.split("//")[1].split("/")[0]
    s3_key = remote.split(bucket_name + '/')[1] # converts path

    if not bucket_conn or not bucket_conn.name == bucket_name:
        bucket_conn = s3_bucket_create(bucket_name)

    k = bucket_conn.get_key(s3_key)

    if return_bucket:
        if k:
            return True, bucket_conn
        else:
            return False, bucket_conn
    else:
        if k:
            return True
        else:
            return False

def sendStatusEmail(to_email="rkraft4@gmail.com", from_email="rkraft4@gmail.com",
                    subject="FORMA data update status", body=None):
    aws_id, aws_key = getAwsConfig()
    ses = SESConnection(aws_id, aws_key)

    return ses.send_email(from_email.strip(), subject, body, to_email.strip())
