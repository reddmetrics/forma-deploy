from ftplib import FTP
import sys
import platform

class ProgressMessage():
    def __init__(self, n=0, interval=100, message="files processed"):
        self.n = n
        self.message = message
        self.interval = interval


    def update(self):
        self.n += 1
        if self.n % self.interval == 0:
            print "%i %s" % (self.n, self.message)
