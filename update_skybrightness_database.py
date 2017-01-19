from datetime import datetime, timedelta
import configparser
import os
import re
import mysql.connector
import numpy as np

config_file = os.path.abspath(__file__)
config_file = re.sub('update_skybrightness_database.py$', 'configurations.ini', config_file)

config1 = configparser.ConfigParser()
config1.read(config_file)

config = {
    'user': config1['MYSQL_CONN']['username'],
    'password': config1['MYSQL_CONN']['password'],
    'host': config1['MYSQL_CONN']['host'],
    'database': config1['MYSQL_CONN']['db'],
    'charset': config1['MYSQL_CONN']['charset']
    }


def is_moon(date_):
    import ephem
    suth = ephem.Observer()

    suth.date = date_

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


def add_day(str_):
    date_ = datetime(int(str_[0:4]), int(str_[4:6]), int(str_[6:])) + timedelta(days=1)
    val = str(date_)[0:4] + str(date_)[5:7] + str(date_)[8:10]
    return val


def get_last_update_date(telescope):
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    if telescope == config['TELESCOPE']['rise_name']:
        return config['LAST_UPDATE']['date_rise']
    elif telescope == config['TELESCOPE']['set_name']:
        return config['LAST_UPDATE']['date_set']
    else:
        return None


def get_path_to_file(tele_, date_, folder_):
    global config_file
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DATA_PATH']['data'] + tele_ + '/' + date_ + '/' + folder_ + '/'


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
    next_day = add_day(current_date)

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
    add_sb = ("INSERT INTO SkyBrightness"
              "(DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE)"
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.executemany(add_sb, skybright)
        cnx.commit()

    except mysql.connector.Error as err:
        if "Duplicate entry" in err.msg:
            print('SB duplicate')
        else:
            print('SB Fail!!')
            print(err.msg, "SB")
        if cnx:
            cnx.rollback()

    finally:
        if cnx:
            cnx.close()


def insert_extinctions(extinctions):
    '''
        :param extinctions:  data to insert to the database
        :return: None
    '''

    add_ext = ("INSERT INTO Extinctions"
               "(DATE_TIME, STAR, EXTINCTION, EXT_ERROR,  X_POS,  Y_POS,  AIRMASS,  FILTER_BAND, TELESCOPE)"
               "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)")

    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.executemany(add_ext, extinctions)
        cnx.commit()

    except mysql.connector.Error as err:
        if "Duplicate entry" in err.msg:
            print('EXT duplict')
        elif "Not enough parameters" in err.msg:
            print('EXT param')
        else:
            print('EXT Fail!!')
            print(err.msg, "EXT")
        if cnx:
            cnx.rollback()

    finally:
        if cnx:
            cnx.close()

'''
def insert_extinctions_reduced(reduced_extinctions):


        :param reduced_extinctions:  data to insert to the database
        :return: None


    cnx = mysql.connector.connect()
    add_ext_red = ("INSERT INTO Ext_Reduced"
                   "(DATE_TIME, EXTINCTION, EXT_ERROR, FILTER_BAND, TELESCOPE, CLOUD_COVERAGE, MOON)"
                   "VALUES(%s, %s, %s, %s, %s, %s, %s)")
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.execute(add_ext_red, reduced_extinctions)

        cnx.commit()

    except mysql.Error as err:
        if "Duplicate entry" in err.msg:
            pass
        elif "Not enough parameters" in err.msg:
            pass
        else:
            print(err.msg, "EXT")
        if cnx:
            cnx.rollback()

    finally:
        if cnx:
            cnx.close()


def update_ext_cc():
    update_ext = ("UPDATE Ext_Reduced e "
                  "SET e.CLOUD_COVERAGE = ( "
                  "     SELECT s.CLOUD_COVERAGE "
                  "     FROM SkyBright s "
                  "     ORDER BY ABS( "
                  "                     TIMESTAMPDIFF(SECOND, s.DATE_TIME, e.DATE_TIME) "
                  "                 ) "
                  "     LIMIT 1 ) "
                  "WHERE e.DATE_TIME > 101 ")

    try:
        cnx = mysql.connector.connect(**config)
        print("connected #")
        cursor = cnx.cursor()
        print("cursor found # ")
        cursor.execute(update_ext)
        cnx.commit()
        print( "commited #")

    except mysql.connector.Error as err:
        print("Error")
        if "Duplicate entry" in err.msg:
            print('EXT Duplict #')
        elif "Not enough parameters" in err.msg:
            print('EXT param #')
        else:
            print('EXT Fail #')
            print(err)
        if cnx:
            print("rolling back #")
            cnx.rollback()

    finally:
        if cnx:
            cnx.close()
            print("one done #")
'''

# ++++++++++++++++ READ directory++++++++
def read_skybrightness(filename, path_to_file):
    '''
        :param filename: name of the file that need to be read
        :param path_to_file: a path to filename
        :return: None
    '''
    full_list = []  # a list of elements to insert to the skybrightness table

    telescope = find_telescope(path_to_file)
    os.chdir(path_to_file)  # change dir to where the file is located
    with open(filename) as f:
        for line in f:
            list_ = []
            try:
                data = line.split()
                # DATE_TIME, SKYBRIGHTNESS, SB_ERROR, MOON, FILTER_BAND, POSX, TELESCOPE, CLOUD_COVERAGE
                list_.append(to_date_and_time(data[0]))                 # date and time
                list_.append(float("{0: .2f}".format(float(data[5]))))  # Sky brightness
                list_.append(float("{0: .2f}".format(float(data[6]))))  # SB error
                list_.append(is_moon(to_date_and_time(data[0])))        # moon
                list_.append(filename[10])                              # filter band
                list_.append(int(filename[22]))                         # position key
                list_.append(telescope)                                 # telescope
                list_.append(float("{0: .1f}".format(float(data[8]) * 100)))  # Cloud coverage

                full_list.append(list_)

            except:
                if len(list_) < 8:
                    print(filename, data[0], "Fail to append list")
                else:
                    print(list_)

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
                full_list.append(list_)

                #deviation_list.append(float("{0: .2f}".format(float(data[3]))))
                #median_list.append(float("{0: .2f}".format(float(data[2]))))
            except:
                pass
    '''
    try:
        if len(median_list) > 0 and len(deviation_list) > 0:
            # DATE_TIME, EXTINCTION, EXT_ERROR, FILTER_BAND, TELESCOPE, CLOUD_COVERAGE(200 default)
            reduced_list.append(full_list[0][0])
            reduced_list.append(float("{0: .2f}".format(np.median(median_list))))
            reduced_list.append(float("{0: .2f}".format(np.std(median_list))))
            reduced_list.append(filter_band)
            reduced_list.append(telescope)
            reduced_list.append(200.0)
            reduced_list.append(is_moon(str(full_list[0][0])))
    except:
        pass
    '''

    if len(full_list) > 0:
        insert_extinctions(full_list)

    #if len(reduced_list) > 0:
    #    insert_extinctions_reduced(reduced_list)


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
            date_ = get_last_update_date(tele_)
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
                    print(Exception)

        is_it_rise = update_last_date(get_last_update_date(tele_), config['TELESCOPE']['rise_name'])
        is_it_set = update_last_date(get_last_update_date(tele_), config['TELESCOPE']['set_name'])
        print(get_last_update_date('astmonsunrise'))

if __name__ == "__main__":
    read_day()
