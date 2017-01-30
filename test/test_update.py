'''
    This is a test for functions in update_skybrightness_database
'''
import update_skybrightness_database
import unittest
import os, re

path_ = os.path.abspath(__file__)
path_ = re.sub('test_data_reading.py$', "", path_)


def create_file(text, filename):
    try:
        os.chdir(path_)
        f = open(filename, 'w')
        for line in text:
            f.write(line)
            f.write("\n")
        f.close()
    except Exception as err:
        print("Fail to create test data! \n\nError is:\n%s" % err)


def remove_file(filename):
    os.remove(filename)


class TestUpdate(unittest.TestCase):
    '''
        Test the is_moon function
    '''
    def test_is_moon(self):
        '''
            Testing is_moon
        '''
        result = update_skybrightness_database.is_moon("2017-01-09 20:00:00")
        self.assertEqual(result, 1)
        result = update_skybrightness_database.is_moon("2016-06-07 17:00:00")
        self.assertEqual(result, 1)

        result = update_skybrightness_database.is_moon("2017-01-04 22:00:00")
        self.assertEqual(result, 0)
        result = update_skybrightness_database.is_moon("2016-06-07 19:00:00")
        self.assertEqual(result, 0)

        result = update_skybrightness_database.is_moon("2017-01-04 25:00:00")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.is_moon("2016-02-30 20:00:00")
        self.assertEqual(result, -1)

    def test_add_date_directory(self):
        '''
            Testing add day
        '''
        result = update_skybrightness_database.next_date_directory("20160303")
        self.assertEqual(result, "20160304")
        result = update_skybrightness_database.next_date_directory("20160331")
        self.assertEqual(result, "20160401")
        result = update_skybrightness_database.next_date_directory("20160229")
        self.assertEqual(result, "20160301")
        result = update_skybrightness_database.next_date_directory("20170228")
        self.assertEqual(result, "20170301")

        result = update_skybrightness_database.next_date_directory("20160230")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.next_date_directory("20170242")
        self.assertEqual(result, -1)

    def test_get_path_to_file(self):
        '''
            Testing get path to file
        '''
        result = update_skybrightness_database.get_path_to_file("astmonsunrise", "20160606", "Extinctions")
        self.assertEqual(result, "/data/astmon/astmonsunrise/2016/20160606/Extinctions/")

        result = update_skybrightness_database.get_path_to_file("astmonsunset", "20170101", "SkyBrightness")
        self.assertEqual(result, "/data/astmon/astmonsunset/2017/20170101/SkyBrightness/")

    def test_to_date_and_time(self):
        '''
            Testing to_date_and_time
        '''
        result = update_skybrightness_database.to_date_and_time("20160601_001429")
        self.assertEqual(result, "2016-06-01 00:14:29")
        result = update_skybrightness_database.to_date_and_time("20160601_232536")
        self.assertEqual(result, "2016-06-01 23:25:36")

    def test_find_telescope(self):
        '''
            Testing if the telescope data can be found.
        '''
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunrise/the/rest")
        self.assertEqual(result, 0)
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunrise/the/rest")
        self.assertNotEqual(result, 1)
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunrise/the/rest")
        self.assertNotEqual(result, -1)

        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunset/the/rest")
        self.assertEqual(result, 1)
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunset/the/rest")
        self.assertNotEqual(result, 0)
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunset/the/rest")
        self.assertNotEqual(result, -1)

        result = update_skybrightness_database.find_telescope("/data/astmon/asunrise/the/rest")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.find_telescope("/da/astmon/astmonsunrise/the/rest")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.find_telescope("/datat/astmon/astmonsunrise/the/rest")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.find_telescope("/data/astmo/astmonsunri/the/rest")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsurise/the/rest")
        self.assertEqual(result, -1)
        result = update_skybrightness_database.find_telescope("/data/astmon/astmonsunris/the/rest")
        self.assertEqual(result, -1)

if __name__ == '__main__':
    unittest.main()


