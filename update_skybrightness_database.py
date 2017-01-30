from datetime import datetime, timedelta
import configparser
import os
import re
import mysql.connector
import smtplib
import sys
from email.mime.text import MIMEText

email = os.environ['SKYBRIGHT_LOGGING_MAIL_TO_ADDRESSES']

config_file = os.path.abspath(__file__)
config_file = re.sub('update_skybrightness_database.py$', 'configurations.ini', config_file)

config1 = configparser.ConfigParser()
config1.read(config_file)

sender = os.environ['SKY_SENDER']
reciever = os.environ['SKY_RECEIVER']


def set_config():
    if __name__ == '__main__':
        config = {
            'user': config1['MYSQL_CONN']['username'],
            'password': config1['MYSQL_CONN']['password'],
            'host': config1['MYSQL_CONN']['host'],
            'database': config1['MYSQL_CONN']['db'],
            'charset': config1['MYSQL_CONN']['charset']
        }
    else:
        config = {
            'user': os.environ['LOCAL_DB_USER'],
            'password': os.environ['LOCAL_DB_PASS'],
            'host': os.environ['TEST_HOST'],
            'database': os.environ['TEST_DB'],
        }
    return config


def logging_error(error_):
    date_ = datetime.now()
    err_msg = "\n\nDate and time: %s \n%s" % (str(date_), error_)
    f = open("Error_log.txt", 'a')
    f.write(err_msg)
    f.close()


def build_message(send, receipt, subject, massage):
    text = massage
    msg = MIMEText(text, 'plain')
    msg['Subject'] = subject
    msg['From'] = send
    msg['To'] = ",".join(receipt)
    return msg


def send_message(msg):
    me = os.environ['SKYBRIGHT_LOGGING_MAIL_TO_ADDRESSES']
    try:
        server = smtplib.SMTP("smtp.saao.ac.za")

        try:
            result = server.sendmail(msg['From'], msg['To'].split(","), msg.as_string())
        finally:
            server.quit()

        return result
    except Exception as exc:
        # logging the error in the log
        err = "send Email Log error\n" \
              " user %s \n" \
              "Exception: %s" % (__name__, exc.args)
        logging_error(err)


def is_moon(date_):
    global email
    import ephem
    suth = ephem.Observer()

    suth.date = date_

    try:
        suth.lon = str(20.810694444444444)
        suth.lat = str(-32.37686111111111)
        suth.elev = 1460

        beg_twilight = suth.next_rising(ephem.Moon(), use_center=True)  # Begin civil twilight
        end_twilight = suth.next_setting(ephem.Moon(), use_center=True)  # End civil twilight
        if end_twilight < beg_twilight:
            beg_twilight = suth.previous_rising(ephem.Moon(), use_center=True)
        return 0 if datetime.strptime(date_, "%Y-%m-%d %H:%M:%S") <= beg_twilight.datetime() \
                    or datetime.strptime(date_, "%Y-%m-%d %H:%M:%S") >= end_twilight.datetime() \
            else 1
    except Exception as exc:
        if __name__ == "__main__":
            massage = ("\nError was found on finding moon of date %s\n\n Exception is %s"
                       "\nplease find out what went wrong.\n\nSkyBright Error Log") % (date_, exc)

            send_message(build_message(email, email, "Skybright: Is Moon Fail", massage))
            logging_error(exc)
        return -1


def next_date_directory(str_):
    try:
        date_ = datetime(int(str_[0:4]), int(str_[4:6]), int(str_[6:])) + timedelta(days=1)
        val = str(date_)[0:4] + str(date_)[5:7] + str(date_)[8:10]
        return val
    except Exception as exc:
        if __name__ == "__main__":
            massage = """
                Directory %s represent no date
                or data in %s is not recorded

                Please find out what went wrong

                Exception: %s
            """ % (str_, str_, exc)

            send_message(build_message(email, email, "Skybright: Next Date Directory", massage))
            logging_error(exc)
            sys.exit(0)
        return -1


def get_last_updated_directory(telescope):
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    if telescope == config['TELESCOPE']['rise_name']:
        return config['LAST_UPDATE']['date_rise']
    elif telescope == config['TELESCOPE']['set_name']:
        return config['LAST_UPDATE']['date_set']
    else:
        return None


def get_path_to_file(tle_, date_, folder_):
    global config_file
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    return cfg['DATA_PATH']['data'] + tle_ + '/' + date_[:4] + '/' + date_ + '/' + folder_ + '/'


def to_date_and_time(date_str):
    year_ = (date_str[0:4])
    month_ = (date_str[4:6])
    day_ = (date_str[6:8])
    hour_ = (date_str[9:11])
    min_ = (date_str[11:13])
    sec_ = (date_str[13:])
    date_string = year_ + '-' + month_ + '-' + day_ + ' ' + hour_ + ':' + min_ + ':' + sec_
    return date_string


def find_telescope(path_to_file):
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    if path_to_file.startswith(config['DATA_PATH']['data'] + config['TELESCOPE']['rise_name']):
        return 0
    elif path_to_file.startswith(config['DATA_PATH']['data'] + config['TELESCOPE']['set_name']):
        return 1
    else:
        return -1


def update_last_date(current_date, telescope):
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    path_ = config['DATA_PATH']['data'] + telescope
    dirs = os.listdir(path_)
    date_ = 'date_rise' if telescope == config['TELESCOPE']['rise_name'] else 'date_set'
    next_day = next_date_directory(current_date)

    for file in dirs:
        if current_date < file and file.startswith('20'):
            configs = configparser.ConfigParser()
            configs.read(config_file)
            configs.set('LAST_UPDATE', date_, next_day)
            with open(config_file, 'w+') as configfile:
                configs.write(configfile)
            return True
    return False


def insert_skybright(skybright):
    '''
        :param skybright: data to insert to the database
        :return: None
    '''

    add_sb = ("INSERT INTO SkyBrightness "
              "(DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE)"
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    config = set_config()

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.executemany(add_sb, skybright)
        conn.commit()

    except mysql.connector.Error as err:

        if "Duplicate entry" in err.msg:
            pass
        else:
            if __name__ == '__main__':
                message = """\nFailed to insert data to the database.\n
                        \nPlease check data and run script manually to update database.\n
                        \nError message:
                        \n%s\n\nData List:\n%s\n
                    """ % (err.msg, skybright)
                send_message(build_message(sender, reciever,
                                           'SkyBright: Fail update database', message))
                logging_error(err.msg)
                if conn:
                    conn.rollback()
                logging_error(err.msg)
                sys.exit(0)
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()


def insert_extinctions(extinctions):
    '''
        :param extinctions:  data to insert to the database
        :return: None
    '''

    add_ext = ("INSERT INTO Extinctions"
               "(DATE_TIME, STAR, EXTINCTION, EXT_ERROR,  X_POS,  Y_POS,  AIRMASS,  FILTER_BAND, TELESCOPE, MOON)"
               "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    try:
        config = set_config()
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.executemany(add_ext, extinctions)
        cnx.commit()

    except mysql.connector.Error as err:
        if "Duplicate entry" in err.msg:
            pass
        else:
            if __name__ == '__main__':
                message = """\nFailed to insert data to the database.\n
                        \nPlease check data and run script manually to update database.\n
                        \nError message:\n%s\n\nData List:\n%s\n
                    """ % (err.msg, extinctions)
                send_message(build_message(sender, reciever,
                                           'SkyBright: Fail update database(Extinctions)', message))
                if cnx:
                    cnx.rollback()
                    logging_error(err.msg)
                sys.exit(0)
        if cnx:
            cnx.rollback()

    finally:
        if cnx:
            cnx.close()


def read_skybrightness(filename, path_to_file):
    '''
        :param filename: name of the file that need to be read
        :param path_to_file: a path to filename
        :return: None
    '''
    full_list = []  # a list of elements to insert to the skybrightness table
    bad_file = False

    telescope = find_telescope(path_to_file)
    os.chdir(path_to_file)  # change dir to where the file is located
    with open(filename) as f:
        for line in f:
            list_ = []
            try:
                data = line.split()
                # DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE
                # this order must be like as in insert SQL query.
                list_.append(to_date_and_time(data[0]))                         # date and time
                list_.append(float("{0: .2f}".format(float(data[5]))))          # Sky brightness
                list_.append(float("{0: .2f}".format(float(data[6]))))          # SB error
                list_.append(int(data[7]))                                      # moon
                list_.append(filename[10])                                      # filter band
                list_.append(int(filename[22]))                                 # position key
                list_.append(telescope)                                         # telescope
                list_.append(float("{0: .1f}".format(float(data[8]) * 100)))    # Cloud coverage

                full_list.append(list_)

            except ValueError:
                pass
            except:
                if __name__ == '__main__':
                    bad_file = True
                    logging_error("this is that error")
    if __name__ != "__main__":
        return full_list
    else:
        if bad_file:
                message = """\nFilename %s contains data not handled properly \n
                        \nPlease check file %s \n Path: %s %s\n
                        and run script manually to continue with database updating.\n
                        \n
                    """ % (filename, filename, path_to_file, filename)
                send_message(build_message(sender, reciever,
                                           'SkyBright: Bad file', message))
                sys.exit(0)
        else:
            insert_skybright(full_list)


def read_extinctions(filename, path_to_file):
    filter_band = filename[11]
    full_list = []
    telescope = find_telescope(path_to_file)

    os.chdir(path_to_file)  # change dir to where the file is located
    with open(filename) as f:
        for line in f:
            list_ = []
            try:
                data = line.split()
                list_.append(to_date_and_time(data[0]))                 # date and time
                list_.append(data[1])                                   # Star value
                list_.append(float("{0: .2f}".format(float(data[2]))))  # Extinction
                list_.append(float("{0: .2f}".format(float(data[3]))))  # Ext error
                list_.append(int(data[4]))                              # X
                list_.append(int(data[5]))                              # Y
                list_.append(float("{0: .2f}".format(float(data[6]))))  # air mass
                list_.append(filter_band)                               # filter band
                list_.append(telescope)                                 # telescope
                list_.append(is_moon(to_date_and_time(data[0])))        # moon

                full_list.append(list_)

            except ValueError:
                pass
            except:
                if __name__ == '__main__':
                    logging_error(str(Exception.args) + "this is that error 2" + __name__)
                    raise
                    pass

    if __name__ != "__main__":
        return full_list

    if len(full_list) > 0:
        insert_extinctions(full_list)


def read_day():
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    is_it_rise = True
    is_it_set = True
    while is_it_rise or is_it_set:
        for p in range(4):
            tele_ = config['TELESCOPE']['rise_name'] if p == 0 or p == 1 else config['TELESCOPE']['set_name']
            folder_ = config['FOLDER']['sky'] if p == 0 or p == 2 else config['FOLDER']['ext']
            date_ = get_last_updated_directory(tele_)
            path = get_path_to_file(tele_, date_, folder_)
            try:

                dirs = os.listdir(path)

                for file in dirs:
                    if folder_ == config['FOLDER']['sky']:
                        if file.endswith('.dat') and file.startswith('SBJohnson'):
                            read_skybrightness(file, path)
                    if folder_ == config['FOLDER']['ext']:
                        if file.endswith('.dat') and file.startswith('ExtJohnson'):
                            read_extinctions(file, path)
            except:
                if FileNotFoundError:
                    pass
                else:
                    logging_error(str(Exception.args) + "this is that error 4")

        is_it_rise = update_last_date(get_last_updated_directory(tele_), config['TELESCOPE']['rise_name'])
        is_it_set = update_last_date(get_last_updated_directory(tele_), config['TELESCOPE']['set_name'])

if __name__ == "__main__":
    read_day()
