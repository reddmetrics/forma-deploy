import ftplib

class FTPConn(ftplib.FTP):
    #    def __init__(self, server, path=None, dirs=None, files=None, infiles=None, outfiles=None, outfile=None, user="", passwd=""):
    #        self.server = server
    #        self.path = path                    #add in, out, scratch
    #        self.dirslist = dirs
    #        self.infiles = infiles
    #        self.outfiles = outfiles
    #        self.outfile = outfile
    #        self.files = files
    #        self.user = user            # default is empty string to match FTP
    #        # library parameter
    #        self.password = passwd    # ditto
    #        self.FTP = None

    #    def login(self):
    #        print "Logging in"
    #        self.FTP = ftplib.FTP(self.server, self.username, self.password)  # attach server connection via ftplib
    #        print self.FTP.getwelcome()
    #        #FYI does not print any server welcome messages for some reason.
    #        # This makes it difficult to figure what's going on if there's a login problem."
    #
    #        print "Successfully logged in"

    #    def cwd(self, path):
    #        """
    #        Change to a given directory.
    #        Path should include leading '/'
    #        """
    #
    #        self.FTP.cwd(path)

    def listdir(self, path, include_path=True, verbose=False):
        """
        List contents of path, including file metadata.
        include_path=True means it'll include the path in the output list. If
         False, you only get the filenames and metadata.
        """

        file_list = []

        self.dir(path, file_list.append)

        if include_path:
            for f in xrange(len(file_list)):
                file_list[f] = path + file_list[f]

        if verbose:
            for f in file_list:
                print f

        return file_list

    def list_nometa(self, path, include_path=True, verbose=False):
        """
        List contents of path, skipping file metadata.
        """
        
        file_list = self.nlst(path)
        
        if include_path:
            file_list = [path + fname for fname in file_list]
            
        """
        else:
            curr_dir = self.FTP.pwd()   # store current directory
            self.FTP.cwd(path)          # move to 'path'
            file_list = self.FTP.nlst() # get filenames
            self.FTP.cwd(curr_dir)      # go back to original directory
        """
        if verbose:
            for f in file_list:
                print f

        return file_list

    def get(self, remote, local):

        #        try:
        #            self.login()
        #        except ftplib.error_perm:
        #            print "Probably already logged in"
        #        print "Downloading", file.name
        self.retrbinary("RETR " + remote, open(local, "wb").write)
        #self.FTP.quit()

        return
