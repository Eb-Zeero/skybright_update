import mysql.connector
import os
import update_database as ud
import functions as fn
import unittest

config = fn.set_config(fn.TEST)


def select_skybright(date_):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    sql = ("SELECT SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE "
           "FROM SkyBrightness "
           "WHERE DATE_TIME = '%s'") % date_
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def select_extinctions(date_):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    sql = ("SELECT STAR, EXTINCTION, EXT_ERROR,  X_POS,  Y_POS,  AIRMASS,  FILTER_BAND, TELESCOPE "
           "FROM Extinctions "
           "WHERE DATE_TIME = '%s'") % date_
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

class TestSkyBrightDatabase(unittest.TestCase):
    """
    Test the Sky bright database
    """

    def setUp(self):
        """
        Setup a temporary table in the test database
        """
        config = fn.set_config(fn.TEST)
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # create a table
        cursor.execute("""CREATE TABLE IF NOT EXISTS Extinctions
                        (DATE_TIME datetime, STAR text, EXTINCTION float, EXT_ERROR float, X_POS int, Y_POS int,
                        AIRMASS float, FILTER_BAND char, TELESCOPE int, MOON int)
                        """)
        conn.commit()
        cursor.execute("""CREATE TABLE IF NOT EXISTS SkyBrightness
                          (DATE_TIME datetime, SKYBRIGHTNESS float, SB_ERROR float,
                           MOON int, FILTER_BAND text, POSX int, TELESCOPE int, CLOUD_COVERAGE float)
                       """)
        conn.commit()

        cursor.close()
        conn.close()

    def tearDown(self):
        """
        Delete the temporary table
        """
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        cursor.execute('DROP TABLE IF EXISTS SkyBrightness; ')
        conn.commit()
        cursor.execute('DROP TABLE IF EXISTS Extinctions; ')
        conn.commit()
        cursor.close()
        conn.close()

    def test_database_connection(self):
        conn = mysql.connector.connect(**config)
        conn.close()
        self.assertTrue(True)

    def test_skyBright_insert(self):
        ud.insert_skybright([['2012-01-02 12:11:00', 20.0, 0.01, 1, 'V', 0, 0, 20.0],
                             ['2012-01-02 12:11:00', 21.0, 0.02, 1, 'V', 0, 0, 20.0]], fn.TEST)

        actual = select_skybright('2012-01-02 12:11:00')
        expected = [(20.0, 0.01, 1, 'V', 0, 0, 20.0), (21.0, 0.02, 1, 'V', 0, 0, 20.0)]
        self.assertListEqual(expected, actual)

    def test_extinctions_insert(self):
        ud.insert_extinctions([['2012-01-02 12:11:00', 'test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0, 0],
                               ['2012-01-02 12:11:00', 'test-star', 1.1, 0.01, 2000, 1000, 1500, 'V', 0, 0],
                               ['2012-01-02 12:11:00', 'test-star', 1.2, 0.01, 2000, 1000, 1500, 'V', 0, 0]
                               ], fn.TEST)

        actual = select_extinctions('2012-01-02 12:11:00')
        expected = [('test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0),
                    ('test-star', 1.1, 0.01, 2000, 1000, 1500, 'V', 0),
                    ('test-star', 1.2, 0.01, 2000, 1000, 1500, 'V', 0)
                    ]
        self.assertListEqual(expected, actual)

    def test_incomplete_insert_sky(self):
        #with self.assertRaises(Exception) as context:

        self.assertRaises(Exception, ud.insert_skybright,
                          [['2012-01-02 12:11:00', 0.01, 1, 'V', 0, 0, 20.0], ['2012-01-02 12:11:00', 21.0, 0.02, 1, ]],
                          fn.TEST)

    def test_incomplete_insert_ext(self):
        with self.assertRaises(Exception) as context:
            ud.insert_extinctions([['2012-01-02 12:11:00', 'test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0],
                                   ['2012-01-02 12:11:00', 2000, 1000, 1500, 'V', 0],
                                   ['2012-01-02 12:11:00', 'test-star', 1.2, 0.01, 2000]
                                   ], mode)
        self.assertRaises(Exception, ud.insert_extinctions,
                          [['2012-01-02 12:11:00', 'test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0],
                           ['2012-01-02 12:11:00', 2000, 1000, 1500, 'V', 0],
                           ['2012-01-02 12:11:00', 'test-star', 1.2, 0.01, 2000]], mode)


if __name__ == '__main__':
    mode = fn.TEST
    unittest.main()
