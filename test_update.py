'''
    This is a test for functions in update_skybrightness_database
'''
import update_skybrightness_database
import unittest


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
        result = update_skybrightness_database.is_moon("2017-01-09 20:00:00")
        self.assertNotEqual(result, 0)
        result = update_skybrightness_database.is_moon("2017-01-04 22:00:00")
        self.assertEqual(result, 0)
        result = update_skybrightness_database.is_moon("2017-01-04 22:00:00")
        self.assertNotEqual(result, 1)

    def test_add_day(self):
        '''
            Testing add day
        '''
        result = update_skybrightness_database.add_day("20160303")
        self.assertEqual(result, "20160304")
        result = update_skybrightness_database.add_day("20160331")
        self.assertEqual(result, "20160401")
        result = update_skybrightness_database.add_day("20160229")
        self.assertEqual(result, "20160301")
        result = update_skybrightness_database.add_day("20170228")
        self.assertEqual(result, "20170301")

    def test_get_last_update_date(self):
        '''
            Testing get last update date
        '''
        result = update_skybrightness_database.get_last_update_date("astmonsunrise")
        self.assertNotEqual(result, None)
        result = update_skybrightness_database.get_last_update_date("astmonsunset")
        self.assertNotEqual(result, None)
        result = update_skybrightness_database.get_last_update_date("sunrise")
        self.assertEqual(result, None)

    def test_get_path_to_file(self):
        '''
            Testing get path to file
        '''
        result = update_skybrightness_database.get_path_to_file("astmonsunrise", "20160606", "Extinctions")
        self.assertNotEqual(result, "/data/astmon/astmonsunrise/20160606/Extinctions")

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

    def test_update_last_date(self):
        '''
            Testing update_last_date
        '''
        result = update_skybrightness_database.update_last_date('31000621', "astmonsunset")
        self.assertEqual(result, False)
        result = update_skybrightness_database.update_last_date('20000621', "astmonsunset")
        self.assertEqual(result, True)
        result = update_skybrightness_database.update_last_date('20160621', "astmonsunset")
        self.assertEqual(result, True)

    def test_database_connection(self):
        '''
            Testing if the database connection is possible
        '''
        config = update_skybrightness_database.config
        cnx = update_skybrightness_database.mysql.connector.connect(**config)
        self.assertTrue(cnx)

    def test_insert_skybright(self):
        '''
            Testing if insertion to the database is possible
        '''
        def read_data(date_):
            quary = ("SELECT SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE "
                     "FROM SkyBrightness "
                     "WHERE DATE_TIME = '%s' ") % date_
            config = update_skybrightness_database.config
            cnx = update_skybrightness_database.mysql.connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute(quary)
            data_ = cursor.fetchall()
            cnx.close()
            return data_

        def remove_test_data(date_):
            quary = ("DELETE FROM SkyBrightness "
                     "WHERE DATE_TIME = '%s' ") % date_
            config = update_skybrightness_database.config
            cnx = update_skybrightness_database.mysql.connector.connect(**config)
            cursor = cnx.cursor()
            cursor.execute(quary)
            cnx.commit()
            cnx.close()
        test_list = [
            ['2012-01-01 12:00:00', 20.10, 0.01, 0, 'V', 0, 0, 29.1],
            ['2013-01-01 12:00:00', 20.11, 0.01, 0, 'B', 0, 0, 29.1],
            ['2014-01-01 12:00:00', 20.12, 0.01, 1, 'I', 0, 1, 29.1],
            ['2015-01-01 12:00:00', 20.13, 0.01, 0, 'R', 0, 1, 29.1]
        ]
        update_skybrightness_database.insert_skybright(test_list)
        print('\n\tInset was successful...')

        result = read_data('2012-01-01 12:00:00')
        self.assertEqual(result, [(20.10, 0.01, 0, 'V', 0, 0, 29.1)])
        result = read_data('2013-01-01 12:00:00')
        self.assertEqual(result, [(20.11, 0.01, 0, 'B', 0, 0, 29.1)])
        result = read_data('2014-01-01 12:00:00')
        self.assertEqual(result, [(20.12, 0.01, 1, 'I', 0, 1, 29.1)])
        result = read_data('2015-01-01 12:00:00')
        self.assertEqual(result, [(20.13, 0.01, 0, 'R', 0, 1, 29.1)])
        print('\tAll test data is in the database...')

        remove_test_data('2012-01-01 12:00:00')
        remove_test_data('2013-01-01 12:00:00')
        remove_test_data('2014-01-01 12:00:00')
        remove_test_data('2015-01-01 12:00:00')
        print("\tTest data was successfully removed...")

if __name__ == '__main__':
    unittest.main()


