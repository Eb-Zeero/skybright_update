import unittest
import functions as fn
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


class TestUpdateFlow(unittest.TestCase):
    def setUp(self):
        '''
        create a fake data to test the hole application
        :return: None
        '''

        good_data = ["20160601_210010   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                     "20160601_210011   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                     "20160601_210012   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                     "20160601_210013   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                     "20160601_210014   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                     "20160601_210015   1275   1261   282.75   85.40   20.01   0.15   0   0.21"
                     ]
        bad_data = ["20160602_220010   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                    "20160602_220011   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                    "20160602_220012   1275   1261   282.75   85.40   #infi   bad    0   bad ",
                    "20160602_220013   1275   1261   282.75   85.40   20.01   0.15   0   0.21",
                    "20160602_220014   1275   1261   282.75   85.40   #infi   bad    0   bad ",
                    "20160602_220015   1275   1261   282.75   85.40   #infi   bad    0   bad "
                    ]
        ext_good_data = ["20160601_210035   test_star   0.38   0.01   735   610   2.26",
                         "20160601_210035   test_star   0.38   0.00   138   292   4.12",
                         "20160601_210035   test_star   0.32   0.00   155   330   4.13",
                         "20160601_210035   test_star   0.43   0.01   171   501   2.61",
                         "20160601_210035   test_star   0.47   0.01   180   623   2.22",
                         "20160601_210035   test_star   0.42   0.01   188   699   2.20",
                         "20160601_210035   test_star   0.48   0.01   190   825   1.88",
                         "20160601_210035   test_star   0.19   0.02   166   658   1.69",
                         "20160601_210035   test_star   0.35   0.01   211   117  2.36"]
        ext_dad_data = ["20160602_210035   test_star   #inf   bad    735   610   bad ",
                        "20160602_210035   test_star   0.38   0.00   138   292   4.12",
                        "20160602_210035   test_star   #inf   bad    155   330   bad ",
                        "20160602_210035   test_star   0.43   0.01   171   501   2.61",
                        "20160602_210035   test_star   #inf   bad    180   623   bad ",
                        "20160602_210035   test_star   0.42   0.01   188   699   2.20",
                        "20160602_210035   test_star   0.48   0.01   190   825   1.88",
                        "20160602_210035   test_star   #inf   bad    166   658   bad ",
                        "20160602_210035   test_star   #inf   bad    211   117  bad "]
        empty_data = ""

        create_file(good_data, "SBJohnson_V20160601Pos0.txt")
        create_file(bad_data, "SBJohnson_V20160602Pos0.txt")
        create_file(empty_data, "SBJohnson_V20160604Pos0.txt")

        create_file(ext_good_data, "ExtJohnson_B20160601-012834.txt")
        create_file(ext_dad_data,  "ExtJohnson_B20160602-012834.txt")
        create_file(empty_data,    "ExtJohnson_B20160603-012834.txt")

    def tearDown(self):
        remove_file('SBJohnson_V20160601Pos0.txt')
        remove_file('SBJohnson_V20160602Pos0.txt')
        remove_file('SBJohnson_V20160604Pos0.txt')

        remove_file('ExtJohnson_B20160601-012834.txt')
        remove_file('ExtJohnson_B20160602-012834.txt')
        remove_file('ExtJohnson_B20160603-012834.txt')

    def test_read_data_file(self):
        result = fn.create_skybrightness('SBJohnson_V20160601Pos0.txt', path_, mode)
        expected = [['2016-06-01 21:00:10', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-01 21:00:11', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-01 21:00:12', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-01 21:00:13', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-01 21:00:14', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-01 21:00:15', 20.01, 0.15, 0, 'V', 0, 0, 21.0]
                    ]
        self.assertListEqual(result, expected)

        result = fn.create_skybrightness('SBJohnson_V20160602Pos0.txt', path_, mode)
        expected = [['2016-06-02 22:00:10', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-02 22:00:11', 20.01, 0.15, 0, 'V', 0, 0, 21.0],
                    ['2016-06-02 22:00:13', 20.01, 0.15, 0, 'V', 0, 0, 21.0]
                    ]
        self.assertListEqual(result, expected)

        result = fn.create_skybrightness('SBJohnson_V20160604Pos0.txt', path_, mode)
        expected = []
        self.assertListEqual(result, expected)

    def test_read_ext(self):
        result = fn.create_extinctions("ExtJohnson_B20160601-012834.txt", path_, mode)
        expected = [['2016-06-01 21:00:35', 'test_star', 0.38, 0.01, 735, 610, 2.26, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.38, 0.00, 138, 292, 4.12, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.32, 0.00, 155, 330, 4.13, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.43, 0.01, 171, 501, 2.61, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.47, 0.01, 180, 623, 2.22, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.42, 0.01, 188, 699, 2.20, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.48, 0.01, 190, 825, 1.88, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.19, 0.02, 166, 658, 1.69, 'B', 0, 0],
                    ['2016-06-01 21:00:35', 'test_star', 0.35, 0.01, 211, 117, 2.36, 'B', 0, 0]
                    ]
        self.assertListEqual(result, expected)

        result = fn.create_extinctions("ExtJohnson_B20160602-012834.txt", path_, mode)
        expected = [['2016-06-02 21:00:35', 'test_star', 0.38, 0.00, 138, 292, 4.12, 'B', 0, 0],
                    ['2016-06-02 21:00:35', 'test_star', 0.43, 0.01, 171, 501, 2.61, 'B', 0, 0],
                    ['2016-06-02 21:00:35', 'test_star', 0.42, 0.01, 188, 699, 2.20, 'B', 0, 0],
                    ['2016-06-02 21:00:35', 'test_star', 0.48, 0.01, 190, 825, 1.88, 'B', 0, 0]
                    ]
        self.assertListEqual(result, expected)

        result = fn.create_extinctions("ExtJohnson_B20160603-012834.txt", path_, mode)
        expected = []
        self.assertListEqual(result, expected)

if __name__ == "__main__":
    mode = fn.TEST
    unittest.main()

