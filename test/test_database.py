import mysql.connector
import os
import update_database as ud
import functions as fn
import unittest


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
        "'2012-01-02 12:11:00', 'test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0, 0"

        cursor.close()
        conn.close()

    def tearDown(self):
        """
        Delete the temporary table
        """
        config = fn.set_config(fn.TEST)
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        cursor.execute('DROP TABLE IF EXISTS SkyBrightness; ')
        conn.commit()
        cursor.close()
        conn.close()

    def test_database_connection(self):
        config = fn.set_config(fn.TEST)
        conn = mysql.connector.connect(**config)
        conn.close()
        self.assertTrue(True)

    def test_skyBright_insert(self):
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

        ud.insert_skybright([['2012-01-02 12:11:00', 20.0, 0.01, 1, 'V', 0, 0, 20.0],
                             ['2012-01-02 12:11:00', 21.0, 0.02, 1, 'V', 0, 0, 20.0]], mode)

        actual = select_skybright('2012-01-02 12:11:00')
        expected = [(20.0, 0.01, 1, 'V', 0, 0, 20.0), (21.0, 0.02, 1, 'V', 0, 0, 20.0)]
        self.assertListEqual(expected, actual)

    def test_extinctions_insert(self):
        config = fn.set_config(mode)

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

        ud.insert_extinctions([['2012-01-02 12:11:00', 'test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0, 0],
                               ['2012-01-02 12:11:00', 'test-star', 1.1, 0.01, 2000, 1000, 1500, 'V', 0, 0],
                               ['2012-01-02 12:11:00', 'test-star', 1.2, 0.01, 2000, 1000, 1500, 'V', 0, 0]
                               ], mode)

        actual = select_extinctions('2012-01-02 12:11:00')
        expected = [('test-star', 1.0, 0.01, 2000, 1000, 1500, 'V', 0),
                    ('test-star', 1.1, 0.01, 2000, 1000, 1500, 'V', 0),
                    ('test-star', 1.2, 0.01, 2000, 1000, 1500, 'V', 0)
                    ]
        self.assertListEqual(expected, actual)


if __name__ == '__main__':
    mode = fn.TEST
    unittest.main()
