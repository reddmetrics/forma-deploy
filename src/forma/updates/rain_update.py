"""
This script updates our archive of PRECL data, and only needs to be run after the first week or so of the month. We have a cron job to run it on the 8th of each month.
"""

import subprocess
from ftplib import FTP
import datetime
import os

from file_obj import FileObj

home = "/home/ubuntu/"
hipchat_message = 'echo "I haz updated: \n%s" | ~/hipchat-cli/hipchat_room_message -t %s -r 42482 -f "UpdaterBot"'

def main(staging_bucket="formastaging"):    
    
    ftp = FTP("ftp.cpc.ncep.noaa.gov")
    ftp.login()

    most_recent = ftp.nlst("/precip/50yr/gauge/0.5deg/format_bin_lnx/")[-1]

    date_str = ftp.sendcmd("MDTM %s" % most_recent).split(" ")[1]
    month_str = date_str[4:6]    
    month = datetime.datetime.today().month

    status = ""

    # if there's new data available, it'll have been updated "this month"

    if int(month_str) == datetime.datetime.today().month:

        print "New data available - downloading now"

        local = os.path.join(home, os.path.split(most_recent)[1])
        ftp.retrbinary("RETR %s" % most_recent, open(local, "wb").write)
        
        print "Download complete"
        
        # cleanup - gzip the file, put it on S3
        subprocess.check_call("gzip %s" % local, shell=True)
        local += ".gz"
        f = FileObj()
        remote = os.path.join("s3://%s/PRECL/" % staging_bucket, os.path.split(local)[1])
        f.put(remote, local)
        os.remove(local)
        status = "Updated rain file %s. Now includes data for month %i" % (remote, month - 1)

    else:
        status = "No new rain data: %s updated in month %s" % (most_recent, month_str)
    print status
    
    if os.getenv("hipchat"):
        try:
            subprocess.check_call(hipchat_message % (status, os.getenv("hipchat")), shell=True)
        except:
            pass
        
if __name__ == "__main__":
    main()

