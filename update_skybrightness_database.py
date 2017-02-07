from datetime import datetime
import os
import functions as fn
import mysql.connector
from mysql.connector import errorcode
import smtplib
import sys
import logging
import configparser

#config_file = os.path.realpath('configurations.ini')
script_dir = os.path.dirname(__file__)
config_file = os.path.join(script_dir, 'configurations.ini')
error_log = os.path.join(script_dir, 'Error_log.txt')

config1 = configparser.ConfigParser()
config1.read(config_file)

sender = config1['EMAIL']['sender']
receiver = [config1['EMAIL']['receiver']]


def logging_error():
    date_ = datetime.now()
    err_msg = "\nDate and time: %s \n" % (str(date_))
    f = open(error_log, 'a')
    f.write(err_msg)
    f.close()


def send_message(msg):
    try:
        server = smtplib.SMTP("smtp.saao.ac.za")

        try:
            result = server.sendmail(msg['From'], msg['To'].split(","), msg.as_string())
        except:
            raise Exception("Fail to send email to( or one of): %s" % msg['To'].split(","))
        finally:
            server.quit()

        return result
    except:
        raise Exception("Fail to send email server is down")


def insert_skybright(skybright_data_list):
    '''
        :param skybright_data_list: data to insert to the database
        :return: None
    '''
    add_sb = ("INSERT INTO SkyBrightness "
              "(DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE)"
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    config = fn.set_config("deployment")

    def check_singles():
        for entry in skybright_data_list:
            try:
                conn2 = mysql.connector.connect(**config)
                cursor2 = conn.cursor()
                cursor2.execute(add_sb, tuple(entry))
                conn2.commit()

            except mysql.connector.Error as e:
                if e.errno == errorcode.ER_DUP_ENTRY:
                    # check if all the entries are in the database.
                    pass
                else:
                    error_log = os.path.realpath('Error_log.txt')
                    logging.basicConfig(level=logging.DEBUG, filename=error_log)
                    logging_error()
                    raise Exception("Unchecked error happened on skybright insert")

                if conn2:
                    conn2.rollback()  # do not commit this if such ever happens

            finally:
                if conn2:
                    conn2.close()

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.executemany(add_sb, skybright_data_list)
        conn.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            # check if all the entries are in the database.
            check_singles()
        else:
            error_log = os.path.realpath('Error_log.txt')
            logging.basicConfig(level=logging.DEBUG, filename=error_log)
            logging_error()
            raise Exception("Unchecked error happened on skybright insert")

        if conn:
            conn.rollback()  # do not commit this if such ever happens

    finally:
        if conn:
            conn.close()


def insert_extinctions(extinctions):
    """
        :param extinctions:  data to insert to the database
        :return: None
    """
    add_ext = ("INSERT INTO Extinctions"
               "(DATE_TIME, STAR, EXTINCTION, EXT_ERROR,  X_POS,  Y_POS,  AIRMASS,  FILTER_BAND, TELESCOPE, MOON)"
               "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    def single_entries():
        for entry in extinctions:
            try:
                config2 = fn.set_config("deployment")
                conn2 = mysql.connector.connect(**config2)
                cursor2 = conn2.cursor()
                cursor2.execute(add_ext, tuple(entry))
                conn2.commit()
            except mysql.connector.Error as e:
                if e.errno ==errorcode.ER_DUP_ENTRY:
                    pass
                else:
                    raise Exception("Unchecked error happened on Extinctions single insert")
                if conn2:
                    conn2.rollback()
            finally:
                if conn2:
                    conn2.close()

    try:
        config = fn.set_config("deployment")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.executemany(add_ext, extinctions)
        conn.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            # confirm if all the line are in the data base
            single_entries()
        else:
            raise Exception("Unchecked error happened on extinctions insert")

        if conn:
            conn.rollback()  # do not commit this if such ever happens

    finally:
        if conn:
            conn.close()


def sky_main(file, path_):
    """

    :param file:
    :param path_:
    :return:
    """

    error_log = os.path.realpath('Error_log.txt')
    logging.basicConfig(level=logging.DEBUG, filename=error_log)


            #  creating data_list
    try:
        data_list = fn.create_skybrightness(file, path_)
    except:
        logging_error()
        logging.exception("Bad file")
        send_message(
            fn.build_message(sender, receiver, "Bad File",
                             fn.message("reading")))
        sys.exit(0)
    try:
        insert_skybright(data_list)
    except:
        logging_error()
        logging.exception("Bad file")
        send_message(
            fn.build_message(sender, receiver, "Database Error",
                             fn.message("insert")))
        sys.exit(0)


def ext_main(file, path_):
    try:
        data_list = fn.create_extinctions(file, path_)
    except:
        logging_error()
        logging.exception("Bad file")
        send_message(
            fn.build_message(sender, receiver, "Bad File",
                             fn.message("reading")))
        sys.exit(0)

    try:
        insert_extinctions(data_list)
    except:
        logging_error()
        logging.exception("Database Error")
        send_message(
            fn.build_message(sender, receiver, "Database Error",
                             fn.message("insert")))
        sys.exit(0)


def start():
    config = configparser.ConfigParser()
    config.read(config_file)
    read_rise = True
    read_set = True

    error_log = os.path.realpath('Error_log.txt')
    logging.basicConfig(level=logging.DEBUG, filename=error_log)

    while read_rise or read_set:
        for dic in range(4):
            i = 0
            tele_ = config['TELESCOPE']['rise_name'] if dic == 0 or dic == 1 else config['TELESCOPE']['set_name']
            dir_ = config['FOLDER']['sky'] if dic == 0 or dic == 2 else config['FOLDER']['ext']
            dir_date_ = fn.get_last_updated_directory(tele_)
            file_path_y = fn.get_path_to_file_with_year(tele_, dir_date_, dir_)  # Files moved to a year folder
            file_path_ = fn.get_path_to_file(tele_, dir_date_, dir_)  # File not in a year folder
            try:
                director_list_y = os.listdir(file_path_y)

                for file in director_list_y:
                    if dir_ == config['FOLDER']['sky']:
                        i += 1
                        print(i, 1, end=' ')
                        if file.endswith('.dat') and file.startswith('SBJohnson'):
                            #  creating data_list
                            sky_main(file, file_path_y)

                    if dir_ == config['FOLDER']['ext']:
                        i += 1
                        print(i, 2, end=' ')
                        if file.endswith('.dat') and file.startswith('ExtJohnson'):
                            ext_main(file, file_path_y)

                director_list = os.listdir(file_path_)
                for file in director_list:
                    if dir_ == config['FOLDER']['sky']:
                        i += 1
                        print(i, 3, end=' ')
                        if file.endswith('.dat') and file.startswith('SBJohnson'):
                            sky_main(file, file_path_)

                    if dir_ == config['FOLDER']['ext']:
                        i += 1
                        print(i, 4, end=' ')
                        if file.endswith('.dat') and file.startswith('ExtJohnson'):
                            ext_main(file, file_path_)
            except:
                if FileNotFoundError:
                    pass
                else:
                    logging_error()
                    logging.exception("Unknown Error")
                    send_message(
                        fn.build_message(sender, receiver, "SkyBright: Unknown Error",
                                         fn.message("something-else")))
                    sys.exit(0)

        read_rise = fn.update_last_date(fn.get_last_updated_directory(tele_), config['TELESCOPE']['rise_name'])
        read_set = fn.update_last_date(fn.get_last_updated_directory(tele_), config['TELESCOPE']['set_name'])
        print(" ")
        print(fn.get_last_updated_directory(tele_))

start()
