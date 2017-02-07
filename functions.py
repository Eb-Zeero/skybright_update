from datetime import datetime, timedelta
import configparser
import os
import smtplib
from email.mime.text import MIMEText

script_dir = os.path.dirname(__file__)
config_file = os.path.join(script_dir, 'configurations.ini')

config1 = configparser.ConfigParser()
config1.read(config_file)

emial = config1['EMAIL']['sender']
sender = config1['EMAIL']['sender']
receiver = config1['EMAIL']['receiver']

DEPLOYMENT = 'DEPLOYMENT'
TEST = 'TEST'


def set_config(mode):
    """
    configure the settings need to run the application
    :TEST mode is only for running the test.
    :param mode: settings required
    :return: database configurations
    """
    if mode == 'TEST':
        config = {
            'user': os.environ['LOCAL_DB_USER'],
            'password': os.environ['LOCAL_DB_PASS'],
            'host': os.environ['TEST_HOST'],
            'database': os.environ['TEST_DB'],
        }
    else:
        config = {
            'user': config1['MYSQL_CONN']['username'],
            'password': config1['MYSQL_CONN']['password'],
            'host': config1['MYSQL_CONN']['host'],
            'database': config1['MYSQL_CONN']['db'],
            'charset': config1['MYSQL_CONN']['charset']
        }
    return config


def build_message(send, receipt, subject, massage):
    """
    Build an email that need to send by update_database.send_message
    :param send: and *@saao.ac.za. email only
    :param receipt: a list [] of emails or tuple () of emails
    :param subject: Subject of the email can be ""
    :param massage: message or body of the email
    :return: MIMEText email
    """
    text = massage
    msg = MIMEText(text, 'plain')
    msg['Subject'] = subject
    msg['From'] = send
    msg['To'] = ",".join(receipt)
    return msg


def is_moon(date_):
    """
    Find if the moon is up or down
    :param date_: string date and time
    :return: 1 if moon up else 0 raise Exception other wise
    """
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
                    or datetime.strptime(date_, "%Y-%m-%d %H:%M:%S") >= end_twilight.datetime() else 1
    except:
        raise Exception("Fail to find a moon for date: %s\n date string %s can not be date" % (date_, date_))


def next_date_directory(str_):
    """
    get the next date directory is if directory is 20120101 then will return 20120102
    :param str_: date directory only in format
    :return: next date directory rise Exception other wise
    """
    try:
        date_ = datetime(int(str_[0:4]), int(str_[4:6]), int(str_[6:])) + timedelta(days=1)
        val = str(date_)[0:4] + str(date_)[5:7] + str(date_)[8:10]
        return val
    except Exception as exc:
        raise Exception("Director not a date %s" % str_)


def get_last_updated_directory(telescope):
    """
    read config.ini and get the last date directory that was checked or read
    :param telescope: telescope in need of this date either astmonsunrise or astmonsunset
    :return: date directory and none otherwise
    """
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    if telescope == config['TELESCOPE']['rise_name']:
        return config['LAST_UPDATE']['date_rise']
    elif telescope == config['TELESCOPE']['set_name']:
        return config['LAST_UPDATE']['date_set']
    else:
        return None


def get_path_to_file_with_year(tle_, date_, folder_):
    """
    return a path to the file moved to a year diectory
    :param tle_: telescope
    :param date_: date directory
    :param folder_: SkyBrightness or Extinctions
    :return: path to file
    """
    global config_file
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    return cfg['DATA_PATH']['data'] + tle_ + '/' + date_[:4] + '/' + date_ + '/' + folder_ + '/'


def get_path_to_file(tle_, date_, folder_):
    """
    return a path to the file moved to a year diectory
    :param tle_: telescope
    :param date_: date directory
    :param folder_: SkyBrightness or Extinctions
    :return: path to file
    """
    global config_file
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    return cfg['DATA_PATH']['data'] + tle_ + '/' + date_ + '/' + folder_ + '/'


def to_date_and_time(date_str):
    """
    make string 20120101_123010 to look like a date
    :param date_str: string as above only
    :return: string date of string as above (2012-01-01 12:30:10)
    """
    year_ = (date_str[0:4])
    month_ = (date_str[4:6])
    day_ = (date_str[6:8])
    hour_ = (date_str[9:11])
    min_ = (date_str[11:13])
    sec_ = (date_str[13:])
    date_string = year_ + '-' + month_ + '-' + day_ + ' ' + hour_ + ':' + min_ + ':' + sec_
    return date_string


def find_telescope(path_to_file, mode):
    """
    :param path_to_file: path to the current file to read
    :return: telescope key 0 Sunrise or 1 Sunset
    """
    if mode == 'TEST':
        return 0
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    if path_to_file.startswith(config['DATA_PATH']['data'] + config['TELESCOPE']['rise_name']):
        return 0
    elif path_to_file.startswith(config['DATA_PATH']['data'] + config['TELESCOPE']['set_name']):
        return 1
    else:
        raise Exception("Telescope key not found")


def update_last_date(current_date, telescope):
    """
    Write the last updated date directory to configurations.ini
    :param current_date: date to write
    :param telescope: telescope updating
    :return: return true if there is another folder greater than current else false
    """
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


def create_skybrightness(filename, path_to_file, mode):
    """
        create list data for skybrightness data to write to the database
        :param filename: name of the file that need to be read
        :param path_to_file: a path to filename
        :return: None
    """
    full_list = []  # a list of elements to insert to the skybrightness table

    telescope = find_telescope(path_to_file, mode)
    os.chdir(path_to_file)  # change dir to where the file is located
    with open(filename) as f:
        for line in f:
            list_ = []
            try:
                data = line.split()
                # DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE
                # this order must be like as in insert SQL query.
                list_.append(to_date_and_time(data[0]))  # date and time
                list_.append(float("{0: .2f}".format(float(data[5]))))  # Sky brightness
                list_.append(float("{0: .2f}".format(float(data[6]))))  # SB error
                list_.append(int(data[7]))  # moon
                list_.append(filename[10])  # filter band
                list_.append(int(filename[22]))  # position key
                list_.append(telescope)  # telescope
                list_.append(float("{0: .1f}".format(float(data[8]) * 100)))  # Cloud coverage

                if len(list_) == 8:
                    full_list.append(list_)
                else:
                    raise Exception("Bad file...")

            except ValueError:
                # Do not insert to a row list if one of the data are are not valid.
                pass
            except:
                raise Exception("Bad file data")
    return full_list


def create_extinctions(filename, path_to_file, mode):
    """
        create list data for extinctions data to write to the database
        :param filename: name of the file that need to be read
        :param path_to_file: a path to filename
        :return: list to write in database
    """
    filter_band = filename[11]
    full_list = []
    telescope = find_telescope(path_to_file, mode)

    os.chdir(path_to_file)  # change dir to where the file is located
    with open(filename) as f:
        for line in f:
            list_ = []
            try:
                data = line.split()
                list_.append(to_date_and_time(data[0]))  # date and time
                list_.append(data[1])  # Star value
                list_.append(float("{0: .2f}".format(float(data[2]))))  # Extinction
                list_.append(float("{0: .2f}".format(float(data[3]))))  # Ext error
                list_.append(int(data[4]))  # X
                list_.append(int(data[5]))  # Y
                list_.append(float("{0: .2f}".format(float(data[6]))))  # air mass
                list_.append(filter_band)  # filter band
                list_.append(telescope)  # telescope
                list_.append(is_moon(to_date_and_time(data[0])))  # moon

                if len(list_) == 10:
                    full_list.append(list_)
                else:
                    raise Exception("File has missing data")
            except ValueError:
                # Do not insert to a row list if one of the data are are not valid.
                pass
            except:
                raise Exception("Bad file data")
    return full_list


def message(exp):
    """
    massage to be send to the sky bright admin
    :param exp: exception caught
    :return: message
    """
    msg = "\nTime: %s\n\nUnknown error occurred please check error log\n\nSkyBright" % str(datetime.now())
    if exp == "reading":
        msg = ("\nTime: %s \n\nThe file being read has unusual data please check the error log for more details.\n"
               "This issue need to be resolved fast \n\nThis email will be send until this Error is resolved\n"
               "Scrip is is stopped from running please run the script manually after the issue is "
               "solved\n\nSkyBright\n") % str(datetime.now())

    if exp == "insert":
        msg = ("\nTime: %s\n\nFail to insert to the database please check error log for more details\n\nThis email "
               "will be send until this Error is resolved\nScrip is is stopped from running please run the script "
               "manually after the issue is solved\n\nSkyBright") % str(datetime.now())

    return msg
