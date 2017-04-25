from datetime import datetime
import os
os.system("which python3")
import functions as fn
import pymysql
import smtplib
import sys
import logging
import configparser

script_file = os.path.dirname(__file__)
script_dir = os.path.join("/home/nhlavutelo", script_file)
config_file = os.path.join(script_dir, 'configurations.ini')
error_log = os.path.join(script_dir, 'Error_log.txt')
config1 = configparser.ConfigParser()
config1.read(config_file)
print("This!!!", config_file,"\nFile!!!", script_file, "\nDir!!!", script_dir, "Name!!", __file__)
sender = config1['EMAIL']['sender']
receiver = [config1['EMAIL']['receiver']]


def error_time():
    """
    Log the date and time an error was logged to the error log
    :return: None
    """
    date_ = datetime.now()
    err_msg = "\nDate and time: %s \n" % (str(date_))
    f = open(error_log, 'a')
    f.write(err_msg)
    f.close()


def send_message(msg):
    """
    Send in param massage this massage must be create by functions.build massage only
    :param msg: massage created by build massage
    :return: Massage sent report
    """
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


def insert_skybright(skybright_data_list, mode):
    """
        write the given iterable data list to the database
        N.B. data must be iterable, e.g. tuple of tuple ie. tuple(tuple(data)), list of list [[data], [data],...]
        :param skybright_data_list: data to insert to the database
        :return: None
    """
    add_sb = ("INSERT INTO SkyBrightness "
              "(DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE)"
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    config = fn.set_config(mode)

    def check_singles(single):
        try:
            conn2 = pymysql.connect(**config)
            cursor2 = conn2.cursor()
            cursor2.execute(add_sb, single)
            conn2.commit()

        except pymysql.Error as e:
            if e.args[0] == 1062:
                # check if all the entries are in the database.
                 pass
            else:
                err_log = os.path.realpath('Error_log.txt')
                logging.basicConfig(level=logging.DEBUG, filename=err_log)
                error_time()
                raise Exception("Unchecked error happened on skybright insert")

            if conn2:
                conn2.rollback()  # do not commit this if such ever happens

        finally:
            if conn2:
                conn2.close()

    try:
        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        cursor.executemany(add_sb, skybright_data_list)
        conn.commit()

    except pymysql.Error as err:
        if err.args[0] == 1062:
            # check if all the entries are in the database.
            for singles in skybright_data_list:
                check_singles(singles)

        else:
            error_log = os.path.realpath('Error_log.txt')
            logging.basicConfig(level=logging.DEBUG, filename=error_log)
            error_time()
            raise Exception("Unchecked error happened on skybright insert")

        if conn:
            conn.rollback()  # do not commit this if such ever happens

    finally:
        if conn:
            conn.close()


def insert_extinctions(extinctions, mode):
    """
        write the given iterable data list to the database
        N.B. data must be iterable, e.g. tuple of tuple ie. tuple(tuple(data)), list of list ie. [[data]]
        :param extinctions: data to insert to the database
        :return: None
    """
    add_ext = ("INSERT INTO Extinctions"
               "(DATE_TIME, STAR, EXTINCTION, EXT_ERROR,  X_POS,  Y_POS,  AIRMASS,  FILTER_BAND, TELESCOPE, MOON)"
               "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    def single_entries(single):
        try:
            config2 = fn.set_config(mode)
            conn2 = pymysql.connect(**config2)
            cursor2 = conn2.cursor()
            cursor2.execute(add_ext, single)
            conn2.commit()
        except pymysql.Error as e:
            if e.args[0] == 1062:
                pass
            else:
                raise Exception("Unchecked error happened on Extinctions single insert")
            if conn2:
                conn2.rollback()
        finally:
            if conn2:
                conn2.close()

    try:
        config = fn.set_config(mode)
        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        cursor.executemany(add_ext, extinctions)
        conn.commit()

    except pymysql.Error as err:
        if err.args[0] == 1062:
            # confirm if all the line are in the data base
            for singles in extinctions:
                single_entries(singles)
        else:
            raise Exception("Unchecked error happened on extinctions insert")

        if conn:
            conn.rollback()  # do not commit this if such ever happens

    finally:
        if conn:
            conn.close()


def sky_main(file, path_):
    """
    Create the data list that need to be inserted to the database and insert it
    send an email to the user is any thing goes wrong
    :param file: a file need to be read
    :param path_: a base path to the file
    :return: None
    """
    try:
        #  creating data_list
        data_list = fn.create_skybrightness(file, path_, mode)
    except:
        error_time()
        logging.exception("Bad file")
        send_message(
            fn.build_message(sender, receiver, "Bad File",
                             fn.message("reading")))
        sys.exit(0)
    try:
        insert_skybright(data_list, mode)
    except:
        error_time()
        logging.exception("Bad file")
        send_message(
            fn.build_message(sender, receiver, "Database Error",
                             fn.message("insert")))
        sys.exit(0)


def ext_main(file, path_):
    """
    Create the data list that need to be inserted to the database and insert it
    send an email to the user is any thing goes wrong
    :param file: a file need to be read
    :param path_: a base path to the file
    :return: None
    """
    try:
        data_list = fn.create_extinctions(file, path_, mode)
    except:
        error_time()
        logging.exception("Bad file")
        send_message(
            fn.build_message(sender, receiver, "Bad File",
                             fn.message("reading")))
        sys.exit(0)

    try:
        insert_extinctions(data_list, mode)
    except:
        error_time()
        logging.exception("Database Error")
        send_message(
            fn.build_message(sender, receiver, "Database Error",
                             fn.message("insert")))
        sys.exit(0)


def start():
    """
    Main method to run the application
    :return: None
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    read_rise = True  # set to read last date read is the was any other line added a night before
    read_set = True

    logging.basicConfig(level=logging.DEBUG, filename=error_log)

    while read_rise or read_set:
        for dic in range(4):
            tele_ = config['TELESCOPE']['rise_name'] if dic == 0 or dic == 1 else config['TELESCOPE']['set_name']
            dir_ = config['FOLDER']['sky'] if dic == 0 or dic == 2 else config['FOLDER']['ext']
            dir_date_ = fn.get_last_updated_directory(tele_)
            file_path_y = fn.get_path_to_file_with_year(tele_, dir_date_, dir_)  # Files moved to a year folder
            file_path_ = fn.get_path_to_file(tele_, dir_date_, dir_)  # File not in a year folder
            try:
                """
                data found in different files and sometimes one file have what the other doesn't have.
                :director_list_y will be data moved to a year folder
                :director_list_y will be data not in the year folder
                """

                director_list_y = os.listdir(file_path_y)
                for file in director_list_y:  # read subdirectory and file in directory path
                    if dir_ == config['FOLDER']['sky']:
                        if file.endswith('.dat') and file.startswith('SBJohnson'):
                            #  creating data_list
                            sky_main(file, file_path_y)

                    if dir_ == config['FOLDER']['ext']:
                        if file.endswith('.dat') and file.startswith('ExtJohnson'):
                            ext_main(file, file_path_y)


            except:
                if FileNotFoundError:
                    pass
                else:
                    error_time()
                    logging.exception("Unknown Error")
                    send_message(
                        fn.build_message(sender, receiver, "SkyBright: Unknown Error",
                                         fn.message("something-else")))
                    sys.exit(0)

            try:

                director_list = os.listdir(file_path_)
                for file in director_list:
                    if dir_ == config['FOLDER']['sky']:
                        if file.endswith('.dat') and file.startswith('SBJohnson'):
                            sky_main(file, file_path_)

                    if dir_ == config['FOLDER']['ext']:
                        if file.endswith('.dat') and file.startswith('ExtJohnson'):
                            ext_main(file, file_path_)
            except:
                if FileNotFoundError:
                    pass
                else:
                    error_time()
                    logging.exception("Unknown Error")
                    send_message(
                        fn.build_message(sender, receiver, "SkyBright: Unknown Error",
                                         fn.message("something-else")))
                    sys.exit(0)
        try:
            read_rise = fn.update_last_date(fn.get_last_updated_directory(tele_), config['TELESCOPE']['rise_name'])
            read_set = fn.update_last_date(fn.get_last_updated_directory(tele_), config['TELESCOPE']['set_name'])
            print(fn.get_last_updated_directory(tele_), datetime.now())  # debugging purpose to see progress
        except:
            error_time()
            logging.exception("Astmon data in use")
            send_message(
                fn.build_message(sender, receiver, "SkyBright: Unknown Error",
                                 fn.message("permission")))
            sys.exit(0)

if __name__ == '__main__':
    mode = fn.DEPLOYMENT
    start()
