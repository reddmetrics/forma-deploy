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
    def __init__(
                 self, name=None, name_original=None, indir=None, outdir=None,
                 tempdir=None, indir_r=None, outdir_r=None, tempdir_r=None,
                 sds=None, layer=None, tile=None, yyyy=None, jjj=None,
                 index=None, period=None, io=None, dataset=None, layers=None,
                 chunk_count=None, full_path=None, full_path_r=None,
                 outfile=None, infile=None, zipped=None, s3_key=None,
                 data=None, contents=list(), done=0, remote=None, local=None,
                 local_in=None, local_out=None, cs=None, exists=0,
                 remote_out=None, archive_file=None):

        self.s3_exists = utils.s3_exists

        self.name = name                    #filename - "mutable"
        self.name_original = name_original  #filename - "immutable"; just want to keep the original filename around for reference
        self.indir = indir                  #local input directory
        self.outdir = outdir                #local output directory
        self.tempdir = tempdir              #local temp directory
        self.indir_r = indir_r              #remote input directory
        self.outdir_r = outdir_r            #remote output directory
        self.tempdir_r = tempdir_r          #remote temp directory
        self.sds = sds                      #scientific dataset (GDAL stuff)
        self.layer = layer
        self.tile = tile                    #MODIS tile of this file
        self.yyyy = yyyy
        self.jjj = jjj
        self.index = index
        self.period = period
        self.io = io
        self.dataset = dataset
        self.layers = layers
        self.chunk_count = chunk_count
        self.full_path = full_path          #full path + filename on local instance
        self.full_path_r = full_path_r      #full path + filename on remote server
        self.zipped = zipped                #
        self.outfile = outfile
        self.infile = infile
        self.s3_key = s3_key                #path on S3
        self.data = data                    # place to store data
        self.contents = contents            # list of contents of an archive
        self.done = done                    # did class method complete successfully?
        self.remote = remote                # full path on remote server; if s3, including s3://; if ftp, including ftp://
        self.local = full_path              # optional parameter defaults to .full_path for compatibility with existing usage
        self.local_in = local_in            # convenient place to store in/out names
        self.local_out = local_out          # convenient place for out names
        self.cs = cs                        # coordinate system
        self.exists = exists                # store return code from checking whether the file exists on S3
        self.remote_out = remote_out        # convenient place to put outfile paths on remote server
        self.archive_file = archive_file    # filename for archiving
        self.ftp = None


    def extract(self, flatten=True, ungzip=True):
        """
        Given an archive of a particular type, extract the contents.
        flatten = True to strip away the directory structure stored
        in the archive and extract everything to a directory named after
        the archive file minus the extension.

        NB this is really only good for working with data files. Compiled
        applications have all sorts of weird naming conventions and delicate
        directory structures that aren't considered at all. The default
        flatten=True is particularly damaging on a Mac, where .app bundles are
        totally ruined.

        Ungzip is just used as an option if there are gzip files inside a
        tarball. If True (the default), any gzip files inside the tarball will
        be extracted into their uncompressed form. If ungzip = False, gzip files
        will be left as such, so that np.loadtxt can read them natively. Idea
        is that we read files half as often if they aren't ungzipped beforehand.
        """
        if self.full_path is None:
            self.full_path = self.local
        ext = self.full_path[-7:]

        if ext == ".tar.gz" or ext[-4:] == ".tar":
            # Definitely a directory
            self.extract_it("tar", flatten, ungzip)

        elif ext[-4:] == ".zip":
            # Could be a zipped directory - assume it is, put it in a directory
            self.extract_it("zip", flatten)

        elif ext[-3:] == ".gz":
            # Individual files
            self.extract_it("gz", flatten)
        else:
            print "Unrecognized compression format for %s" % self.name
            print "Supported formats:"
            print ".tar.gz\n.tar\n.zip\n.gz"

    def extract_it(self, fmt, flatten, ungzip=True):
        """This will overwrite any existing files."""

        #========================#
        # Setup output directory #
        #========================#
        if fmt == "gz":
            dir_out = os.path.split(self.local)[0] + "/"

        else:

            dir_out = os.path.splitext(self.local)[0]

            # in case of multi-extension file (e.g. .tar.gz or .txt.gz)
            while len(os.path.splitext(dir_out)[1]) > 0:
                dir_out = os.path.splitext(dir_out)[0]

            dir_out += "/"

        print "\nExtracting %s to %s" % (os.path.split(self.local)[1], dir_out)

        if not os.path.exists(dir_out):
            os.makedirs(dir_out)

        #=====================#
        # Process zip archive #
        #=====================#

        if fmt == "zip":
            """
            try:
                from zipfile import ZipFile


                z = ZipFile(self.full_path, "r")

                for f in z.namelist():
                    if not flatten:
                        z.extract(f, path=dir_out)
                        self.contents.append(f)
                    else:
                        z1 = z.open(f, "r")
                        filename = os.path.split(f)[1]     # just keep the filename
                        if filename:
                            f = open(dir_out + filename, "w")  # open a file object w/filename f
                            f.write(z1.read())                 # write zipfile member z1 to f
                            f.close()
                            z1.close()
                            self.contents.append(filename)
                        else:
                            # if not filename, it's a directory: 0-length split()[1]
                            if not os.path.exists(dir_out + f):
                                os.makedirs(dir_out + f)
                    z.close()
            except:
            """
            subprocess.check_call(["unzip", self.full_path, "-d", os.path.split(self.full_path)[0] + "/"])

            self.done = 1

        #============================#
        # Process tar/tar.gz archive #
        #============================#

        if fmt == "tar":

            t = tarfile.open(self.full_path, "r")   # w/transparent compression

            for tar in t.getnames():
                if t.getmember(tar).isdir():
                    # Don't want to extract directories - need to create them
                    if not flatten:
                        if not os.path.exists(dir_out + tar):
                            os.makedirs(dir_out + tar)
                elif t.getmember(tar).isfile():
                    if not flatten:
                        full_path = dir_out + tar
                    else:
                        full_path = dir_out + os.path.split(tar)[1]

                    # Open output file, write data

                    if tar: # only does anything if not NoneType
                        f = open(full_path, "w")
                        f.write(t.extractfile(tar).read())
                        f.close()
                        self.contents.append(tar)

                        if tar[-3:] == ".gz" and ungzip == True:
                            gz = gzip.open(full_path)
                            f = open(full_path[:-3], "w")
                            f.write(gz.read())
                            f.close()
                            gz.close()
                            os.remove(full_path)
                            if not flatten:
                                self.contents.append(full_path)
                            else:
                                self.contents.append(os.path.split(tar)[1])
                else:
                    print
                    print "Archive member %s skipped" % tar
                    print "Neither file nor directory type"
                    if t.getmember(tar).islnk():
                        print "Member type: LNKTYPE"
                    elif t.getmember(tar).issym():
                        print "Member type: SYMTYPE"
                    else:
                        print "Member type code: %s" % t.getmember(tar).type
                    print
            t.close()

            self.done = 1

        #======================#
        # Process gzip archive #
        #======================#

        if fmt == "gz":
            host_os = platform.system()
            if host_os == "Linux" or host_os == "Darwin":
                subprocess.check_call(["gunzip", self.local])
                # remove .gz from filename
                self.local = self.local[:-3]
            """
            gz = gzip.open(self.full_path, "r")
            f = open(self.full_path[:-3], "w")
            f.write(gz.read())
            self.contents = os.path.split(self.full_path[:-3])[1]
            """
            self.done = 1

        #==================================================================#
        # Reset self.full_path to new directory containing extracted files #
        #==================================================================#

        if self.done:
            if fmt != "gz":
                self.full_path = dir_out
                # may be able to do something like this, but I hope it doesn't
                # break any older uses of .archive()
                self.local = dir_out + fmt

    def ftp_init(self, server, user, passwd, reset=False):
        if not self.ftp or reset==True:
            print "\nInitializing FTP connection for download"
            self.ftp = FTPConn(server, user=user, passwd=passwd)
            try:
                self.ftp.login()
            except ftplib.error_perm:
                print "Already logged in\n"
        return

    def get(self, remote=None, local=None, user="anonymous", password="anonymous@",
            overwrite=False, params=None):
        if remote:
            self.remote = remote
        if local:
            self.local = local
        self.transfer(user, password, direction="get",
                      overwrite=overwrite, params=params)
        return

    def put(self, remote=None, local=None, rrs=False, params=None):
        if remote:
            self.remote = remote
        if local:
            self.local = local

        self.transfer(user="anonymous", password="anonymous@",
                      direction="put", rrs=rrs, params=params)
        return

    def transfer(self, user, password, direction=None, rrs=False,
                 overwrite=False, params=None):
        # Requires s3cmd
        # Available from http://s3tools.org/s3cmd

        # Make sure needed parameters are set
        if not self.remote:
            print "remote path must be defined to transfer file."

        if not self.local:
            print "local path must be defined to transfer file."

        if not self.remote and self.local:
            print "Needed file parameters undefined. Exiting."
            exit()

        else:
            # Do the remote retrieve magic
            # Currently only for S3 & ftp, but expand protocols list as needed

            protocols = {"s3":["get", "put"], "ftp":["get"]}

            protocol = self.remote.split(":")[0]

            # Check to see if protocol is in protocols - if not, fail
            if protocol not in protocols or direction not in protocols[protocol]:
                print "File location and/or transfer protocol not understood:"
                print self.remote
                print "File not transfered."
                print "Protocols understood:"
                print "\n".join("%s: %s" % (p, " and ".join(protocols[p])) for p in protocols)
                print "\nExiting script"
                exit()

            else:
                if direction == "get":
                    if protocol == "ftp":

                        server = self.remote.split("ftp://")[1].split("/")[0]

                        # if doing repeated ftp transfers, it seems to be worth storing the connection
                        # to the server. Otherwise you're logging in each time and it can get slow

                        self.ftp_init(server, user, password)

                        print "Downloading %s" % self.remote
                        # connection to server could theoretically break
                        tries_max = 5
                        tries = 0
                        while tries < tries_max:
                            try:
                                self.ftp.get(self.remote.split(server)[1], self.local)
                                self.done = 1
                                return

                            except socket.error, EOFError:
                                print "\nConnection error, trying again"
                                if tries > 2:
                                    print "Re-initializing FTP connection"
                                    self.ftp_init(server, user, password, reset=True)
                                tries += 1

                    elif protocol == "s3":
                        cmd = ["s3cmd", "get", self.remote, self.local]
                        if overwrite:
                            cmd.insert(2, "--force")
                    else:
                        pass

                elif direction == "put":
                    if protocol == "s3":
                        cmd = ["s3cmd", "put", self.local, self.remote]
                        if rrs:
                            cmd.insert(1, "-rr")

                else:
                    print "Direction of transfer not defined"
                    exit()


                if params:
                    # insert each of the parameters at position 1, and get flat list
                    # cmd = [1,2,3,4,5]
                    # params = [10, 11, 12]
                    # junk = [cmd.insert(i + 1, params[i]) for i in range(len(params))]
                    # cmd -> [1, 10, 11, 12, 2, 3, 4, 5]

                    [cmd.insert(i + 1, params[i]) for i in range(len(params))]

                if protocol == "s3":

                    if platform.system() == 'Windows':
                        # Workarounds for command line and install issues
                        cmd[0] = static.paths["s3cmd"] # replace 's3cmd' w/path to s3cmd
                        cmd.insert(0, 'python') # insert python call @ front
                        # cmd.insert(2, "--no-progress")

                    subprocess.check_call(cmd)
                self.done = 1
                self.full_path = self.local

                return
    """
    def s3_exists(self, aws, remote=None):
        """"""
        Check whether a particular file already exists on S3

        Requires converting path for s3cmd to format appropriate for boto
        e.g. 's3://forma_test/data/temp/modis/raw/1000/MOD13A3.A2000032.h33v09.005.2006271173933.hdf'
        becomes
        'data/temp/modis/raw/1000/MOD13A3.A2000032.h33v09.005.2006271173933.hdf'
        """"""

        # in case self.remote is already occupied something
        # we'll restore it
        self.temp = self.remote

        if not aws.s3:
            aws.s3 = S3Connection(aws.access_key, aws.secret_key)

        bucket = self.remote.split("//")[1].split("/")[0]

        # This tweak makes the function bucket-agnostic
        # not aws.b means there's no bucket connection at all, so let's create
        # it using the bucket of interest.
        # aws.b.name != bucket means the bucket in self.remote is different
        # from the current aws.b bucket connection, so we need a new connection.
        # This might break some other stuff ...

        if not aws.b or aws.b.name != bucket:
            aws = utils.s3_bucket_create(aws, bucket)

        self.s3_key = self.remote.split(bucket + '/')[1] # converts path

        if aws.s3 and aws.b:
            try:
                a = aws.b.get_key(self.s3_key)
                if not a:
                    # todo return 0 would make it possible to do check for
                    # file on s3 using one line instead of checking self.exists
                    self.exists = 0
                    exists = 0
                else:
                    self.exists = 1
                    exists = 1
            except:
                raise ValueError("Error checking whether file % exists on S3." % self.s3_key)
        else:
            aws.done = 0
            raise ValueError("Error creating S3 connection or bucket instance.")

        self.remote = self.temp

        return exists
    """

    def archive(self, fmt=None):
        """
        Adds file(s) to a compressed tarball (.tar.gz) or gzip file (.gz)
        Does not actually use fmt parameter in current form (8/30/2010)
        """
        if not os.path.exists(self.local):
            error = "File %s does not exist in file system" % self.local
            raise ValueError(error)

        host_os = platform.system()

        if os.path.isfile(self.local):
            fmt = '.gz'
            #fmt = '.tar.gz' # having some weird problems with gzip files, (my
                            # fault no doubt) so .tar.gz appears more reliable.

            if host_os == "Linux" or host_os == "Darwin":
                print "Archiving"
                subprocess.check_call(["gzip", self.local])
            else:
                print "Can't gzip on Windows yet - talk to Robin"
                exit()

        elif os.path.isdir(self.local):
            fmt = ".tar.gz"
            if host_os == "Windows":
                raise ValueError("Windows not supported.")
            else:
                cwd = os.getcwd()

                # to deal with trailing slash that may or may not be in path
                # for ['/Users/robin/data/admin_2708']
                #     ['', 'Users', 'robin', 'data', 'admin_2708']
                # for ['/Users/robin/data/admin_2708/']
                #     ['', 'Users', 'robin', 'data', 'admin_2708', '']

                dirs = self.local.split('/')

                if dirs[-1] == "":
                    # trailing slash - last element of list is empty string
                    dirs = dirs[:-1]

                path, dir = "/".join(dirs[:-1]) + "/", dirs[-1]

                print "Archiving %s" % self.local
                os.chdir(path)
                subprocess.check_call(["tar", "-cvzf", dir + fmt, dir])
                # ex. tar -czvf rain_asc.tar.gz rain_asc

                os.chdir(cwd)
                self.local = path + dir

            print 'Archived %s as %s' % (self.local, self.local + fmt)

        else:
            error = "Unrecognized type: %s is neither a directory or a file" % self.local
            raise ValueError(error)

        self.local += fmt
        print 'Archiving complete: %s' % self.local

    def delete(self):
        os.remove(self.local)
        print "\nDeleted %s" % self.local

    def s3list(self):
        return

    def s3_move(self, new_key):
        cmd = ["s3cmd", "mv", self.remote, new_key]
        subprocess.check_call(cmd)

        return
