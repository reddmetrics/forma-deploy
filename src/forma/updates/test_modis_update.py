import unittest
from forma.testing import modis_update

from forma.staging.cloud import forma_init

aws, d = forma_init.init()

class testModisUpdate(unittest.TestCase):

    def setUp(self):
        self.bucket = "modisfiles"
        self.dataset = "MOD13A3"

        self.modis_ftp = "e4ftl01u.ecs.nasa.gov"
        self.modispath = "/MODIS_Composites/MOLT/%s.005/" % self.dataset
        self.modisdate = "2000.02.01"
        self.modisdate_iso = self.modisdate.replace(".", "-")
        self.fname = "MOD13A3.A2000032.h08v06.005.2007111065956.hdf"

        self.modisfulldatepath = self.modis_ftp + self.modispath + self.modisdate + "/"

        self.dates = ['2000.02.01', '2002.03.01', '2007.04.01', '2009.05.01', '2011.04.01']
        self.dates_dict = dict()
        modisdates = [modisdate.replace(".", "-") for modisdate in self.dates]

        for modisdate in modisdates:
            self.dates_dict[modisdate] = False


        self.modisdatebad = "2000.02.111"
        self.modisfulldatepath = self.modispath + self.modisdate + "1/"

        self.dateerrors = []
        self.fileerrors = {}
        self.filesuccesses = {}

        pass

    def test_genFtpPath(self):

        modisfile = self.modispath + self.modisdate + "/" + self.fname
        ftppath, localpath, localbasepath, s3path = modis_update.genPaths(modisfile,
                                                           self.modisdate_iso,
                                                           self.dataset)

        correctFtpPath = "ftp://%s%s%s/%s" % (self.modis_ftp,
                                              self.modispath,
                                              self.modisdate,
                                              self.fname)

        self.assertEqual(ftppath, correctFtpPath)

    def test_genLocalPath(self):

        modisfile = self.modispath + self.modisdate + "/" + self.fname
        ftppath, localpath, localbasepath, s3path = modis_update.genPaths(modisfile,
                                                           self.modisdate_iso,
                                                           self.dataset)

        correctLocalPath = "%s%s/%s" % (aws.paths.temp, self.modisdate_iso,
                                      self.fname)

        self.assertEqual(localpath, correctLocalPath)

    def test_genS3Path(self):
        modisfile = self.modispath + self.modisdate + "/" + self.fname
        ftppath, localpath, localbasepath, s3path = modis_update.genPaths(modisfile,
                                                           self.modisdate_iso,
                                                           self.dataset)

        correctS3Path = "s3://%s/%s/%s/%s" % (self.bucket, self.dataset,
                                              self.modisdate_iso,
                                              self.fname)

        self.assertEqual(s3path, correctS3Path)

    def test_getModisDatesList(self):
        results = modis_update.Results()
        ftp_base_path = "/MODIS_Composites/MOLT/%s.005/" % self.dataset
        ftp, results = modis_update.getModisDatesList(self.modis_ftp,
                                                      ftp_base_path, results)

        self.assertGreater(len(results.success["dates"]), 0)
        #print results.success["dates"]
        
    def test_getModisDatesListBadFtp(self):
        results = modis_update.Results()
        # add 1 to ftp server address, should cause failure
        ftp_base_path = "/MODIS_Composites/MOLT/%s.005/" % self.dataset
        ftp, results = modis_update.getModisDatesList(self.modis_ftp + "1",
                                                      ftp_base_path, results)
        self.assertGreater(len(results.error["ftp"]), 0)
        #print "FTP error:", results.error["ftp"]

    def test_validateDirectoryDates(self):
        results = modis_update.Results()
        results.success["dates"] = self.dates
        results = modis_update.validateDirectoryDates(results)
        testdate = self.dates[4].replace(".", "-")
        self.assertTrue(testdate in results.get["dates"])

    def test_validateDirectoryDatesOneBad(self):
        # test what happens when one date is invalid for date type

        results = modis_update.Results()
        # TODO: figure out why ftp error seems to be contaminating test
        results.success["dates"] = self.dates
        results.success["dates"][0] += "999"
        baddate = results.success["dates"][0]

        results = modis_update.validateDirectoryDates(results)
        # TODO: figure out how 2011-05-01 is getting in here - should be isolated from other tests
        #print results.get["dates"]
        self.assertTrue(baddate not in results.get["dates"].keys())

    def test_validateDirectoryDatesOneBadErrorCount(self):
        # test whether the number of errors is correct

        results = modis_update.Results()
        results.success["dates"] = self.dates
        results.success["dates"][0] += "999"

        results = modis_update.validateDirectoryDates(results)

        self.assertEquals(len(results.error["dates"]), 1)

    def test_simpleS3FileCheckNoMissing(self):
        # test whether the s3 checker correctly finds at least one file in
        # a directory we know we have
        results = modis_update.Results()
        results.get["dates"] = self.dates_dict
        results = modis_update.simpleS3FileCheck(results, self.dataset)

        self.assertFalse(results.get["dates"][self.dates_dict.keys()[0]])

    def test_simpleS3FileCheckOneMissing(self):
        # test whether the s3 checker correctly finds at least one file in
        # a directory we know we have
        results = modis_update.Results()
        results.get["dates"] = self.dates_dict
        results.get["dates"]["2011-05-01"] = False
        results = modis_update.simpleS3FileCheck(results, self.dataset)

        self.assertTrue(results.get["dates"]["2011-05-01"])

if __name__ == "__main__":
    unittest.main()

