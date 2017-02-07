'''
    This is a test for functions in fn
'''
import functions as fn
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
        result = fn.is_moon("2017-01-09 20:00:00")
        self.assertEqual(result, 1)
        result = fn.is_moon("2016-06-07 17:00:00")
        self.assertEqual(result, 1)

        result = fn.is_moon("2017-01-04 22:00:00")
        self.assertEqual(result, 0)
        result = fn.is_moon("2016-06-07 19:00:00")
        self.assertEqual(result, 0)

    def test_add_date_directory(self):
        '''
            Testing add day
        '''
        result = fn.next_date_directory("20160303")
        self.assertEqual(result, "20160304")
        result = fn.next_date_directory("20160331")
        self.assertEqual(result, "20160401")
        result = fn.next_date_directory("20160229")
        self.assertEqual(result, "20160301")
        result = fn.next_date_directory("20170228")
        self.assertEqual(result, "20170301")



    def test_to_date_and_time(self):
        '''
            Testing to_date_and_time
        '''
        result = fn.to_date_and_time("20160601_001429")
        self.assertEqual(result, "2016-06-01 00:14:29")
        result = fn.to_date_and_time("20160601_232536")
        self.assertEqual(result, "2016-06-01 23:25:36")



if __name__ == '__main__':
    unittest.main()


