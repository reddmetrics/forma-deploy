# check whether there are any holes in the MODIS timeseries on S3
# as of June 8, 2011, there were none for the tiles we care about

import sys
sys.path.append("../../../")
from datetime import date, timedelta

try:
    from forma.config import static
    from forma.utils import utils
    from forma.classes.misc import ProgressMessage

except ImportError:
    import static
    import utils
    from misc import ProgressMessage

def getBasedate(interval):
    # get things started with base date
    if interval == 16:
        return date(2000, 2, 18)
    if interval == 32:
        return date(2000, 2, 1)

def cleanDates(dates, interval):
    # turn 2000-01-01 etc. into date objects for date math
    datelist = []
    for dt in dates:
        yyyy, mm, dd = dt.split("-")
        yyyy, mm, dd = int(yyyy), int(mm), int(dd)
        dateobj = date(yyyy, mm, dd)
        datelist.append(dateobj)
    datelist.sort()

    basedate = getBasedate(interval)

    dp = basedate # container for previous date, starts with basedate

    return datelist, dp

def checkMissingDates(files_n_tiles, interval):
    print "\nChecking for missing dates\n"
    msgs = []
    missing_dates = []
    datelist = []
    for tile in files_n_tiles.keys():
        datelist, dp = cleanDates(files_n_tiles[tile], interval)

        for dt in datelist:
            # subtract current date from previous date (dp)
            diff = dt - dp
            # should be one interval apart, or less (e.g. end of year);
            if diff.days > interval and tile in static.tiles:
                msg = "Difference in dates > %i for tile %s\n" % (interval, tile)
                msg += "Current: %s\n" % dt.isoformat()
                msg += "Previous: %s\n" % dp.isoformat()
                msg += "Difference: %d days\n" % diff.days
                msg += "Missing: %s\n" % ((dt - timedelta(interval)).isoformat())
                print msg
                msgs.append(msg)
                missing_dates.append(dt)
            dp = dt
    if len(missing_dates) == 0:
        print "No missing dates\n"

    return missing_dates, datelist, "\n".join(msgs)

def parseFiles(l):
    print "Processing file list"
    outdict = {}
    tile_tally = {}

    p = ProgressMessage(interval=1000)
    for fname in l:
        f = fname.name

        # skip non-standard filenames we don't care about
        if len(f.split("/")) == 3:
            dataset, period, fname = f.split("/")
        else:
            print "Not processed", f
            continue

        tile = utils.getModisTileFromFname(fname)

        if not tile in tile_tally:
            tile_tally[tile] = 0
        tile_tally[tile] += 1

        # make container for period in outdict
        if not tile in outdict:
            outdict[tile] = []
        outdict[tile].append(period)
        p.update()


    return tile_tally, outdict

def checkMissingTiles(tile_tally, datelist):
    msgs = []
    print "\nChecking for missing tiles\n"
    missing_tiles = []
    extra_tiles = []
    for tile, tile_count in tile_tally.items():
        if tile_count < len(datelist) and tile in static.tiles:
            msg = "Tile %s missing %i item(s)" % (tile,
                                                len(datelist) - tile_tally[tile])
            print msg
            msgs.append(msg)
            missing_tiles.append(tile)
        elif tile not in static.tiles:
            extra_tiles.append(tile)
    #    if len(extra_tiles) > 0:
    #        extra_tiles = extra_tiles.sort()
    #        print "Extra files for the following tiles:"
    #        for tile in extra_tiles:
    #            print tile, tile_tally[tile]
    if len(missing_tiles) == 0:
        print "No missing tiles\n"
    return missing_tiles, "\n".join(msgs)

def main(product_prefix, interval, email=False):

    product_prefix = product_prefix.split(".")[0] # drop .005 from raw prefix

    # get a list of all MODIS files
    bucket = utils.s3_bucket_create("modisfiles")
    print "Getting file list for %s\n" % product_prefix
    l = bucket.list("%s/" % product_prefix)

    tile_tally, files_n_tiles = parseFiles(l)
    
    missing_dates, datelist, msg1 = checkMissingDates(files_n_tiles, interval)
    missing_tiles, msg2 = checkMissingTiles(tile_tally, datelist)

    if email:
        utils.sendStatusEmail(subject="Timeseries check update", body="\n".join([msg1, msg2]))

    return missing_dates, missing_tiles

if __name__ == "__main__":

    import sys
    product_prefix, res, interval = utils.cl_modis_product_parser(sys.argv)

    main(product_prefix, interval, email=True)
