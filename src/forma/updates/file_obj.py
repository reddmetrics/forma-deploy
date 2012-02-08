import os, platform, socket, subprocess, tarfile, gzip, ftplib

import sys
sys.path.append("../../")

try:
    from forma.utils import utils
    from forma.config import static
    from ftp import FTPConn
except ImportError:
    import utils
    import static
    from ftp import FTPConn

class FileObj:
    
    def __init__(self):
        self.delete = utils.files.delete
        self.s3Move = utils.files.s3Move
        self.archive = utils.files.archive
        self.get = utils.transfer.get
        self.put = utils.transfer.put
