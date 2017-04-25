import pymysql
import os
from datetime import datetime

tm = datetime.now()
config = {
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASS'],
    'host': os.environ['TEST_HOST'],
    'database': os.environ['TEST_DB'],
}


def create_table_sb(table_name):
    create = " CREATE TABLE {table_name}(" \
             "     Id INTEGER PRIMARY KEY AUTO_INCREMENT, " \
             "     DateTime DATETIME NOT NULL, " \
             "     SkyBrightness FLOAT NOT NULL, " \
             "     SBError FLOAT NOT NULL " \
             " ) ".format(table_name=table_name)
    alt = " ALTER TABLE {table_name} ADD UNIQUE INDEX( " \
          "     DateTime, " \
          "     SkyBrightness, " \
          "     SBError " \
          " ) ".format(table_name=table_name)
    drop = " DROP TABLE IF EXISTS {table_name}".format(table_name=table_name)
    conn = pymysql.connect(**config)
    try:
        cursor = conn.cursor()
        cursor.execute(drop)
        cursor.execute(create)
        cursor.execute(alt)
        conn.commit()
    except pymysql.Error as err:
        print(err)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def create_table_ext(table_name):
    create = " CREATE TABLE {table_name}(" \
             "     Id INTEGER PRIMARY KEY AUTO_INCREMENT, " \
             "     DateTime DATETIME NOT NULL, " \
             "     Star VARCHAR(20) NOT NULL, " \
             "     Extinction FLOAT NOT NULL, " \
             "     ExtError FLOAT NOT NULL, " \
             "     XPos INT NOT NULL, " \
             "     YPos INT NOT NULL, " \
             "     AirMass FLOAT NOT NULL " \
             " ); ".format(table_name=table_name)
    alt = " ALTER TABLE {table_name} ADD UNIQUE INDEX( " \
          "     DateTime, " \
          "     Star, " \
          "     Extinction, " \
          "     ExtError " \
          " ); ".format(table_name=table_name)
    drop = " DROP TABLE IF EXISTS {table_name};".format(table_name=table_name)
    conn = pymysql.connect(**config)
    try:
        cursor = conn.cursor()
        cursor.execute(drop)
        cursor.execute(create)
        cursor.execute(alt)
        conn.commit()
    except pymysql.Error as err:
        print(err)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def create_moon_table():
    create = " CREATE TABLE MoonAndCoverage(" \
             "     Id INTEGER PRIMARY KEY AUTO_INCREMENT, " \
             "     DateTime DATETIME NOT NULL, " \
             "     Moon BOOLEAN NOT NULL, " \
             "     CloudCoverage FLOAT NOT NULL, " \
             "     PosX INT NOT NULL, " \
             "     FilterBand VARCHAR(1) NOT NULL, " \
             "     Telescope INT NOT NULL" \
             " ) "
    alt = " ALTER TABLE MoonAndCoverage ADD UNIQUE INDEX( " \
          "     DateTime, " \
          "     CloudCoverage, " \
          "     PosX, " \
          "     FilterBand, " \
          "     Telescope" \
          " ); "
    drop = " DROP TABLE IF EXISTS MoonAndCoverage;"
    conn = pymysql.connect(**config)
    try:
        cursor = conn.cursor()
        cursor.execute(drop)
        cursor.execute(create)
        cursor.execute(alt)
        conn.commit()
    except pymysql.Error as err:
        print(err)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def create_position_table():
    create = " CREATE TABLE Positions(" \
             "      Id INTEGER PRIMARY KEY AUTO_INCREMENT, " \
             "      PosX INT NOT NULL, " \
             "      XPos INT NOT NULL, " \
             "      YPos INT NOT NULL, " \
             "      Azimuth FLOAT NOT NULL, " \
             "      Altitude FLOAT NOT NULL); "

    drop = " DROP TABLE IF EXISTS Positions;"
    conn = pymysql.connect(**config)
    try:
        cursor = conn.cursor()
        cursor.execute(drop)
        cursor.execute(create)
        conn.commit()
    except pymysql.Error as err:
        print(err)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def get_telescope(telescope):
    return 'Sunrise'if telescope == 0 else 'Sunset'


def get_filter_band(band):
    return 'V' if band == 0 else 'B' if band == 1 else 'R' if band == 2 else 'I'


def create_tables():
    print("SBJ:", datetime.now())
    for tel_ in range(2):
        for pos_ in range(5):
            for fil_ in range(4):
                table_name = 'SBJohnson' + get_telescope(tel_) + get_filter_band(fil_) + 'Pos' + str(pos_)
                create_table_sb(table_name)
                print(datetime.now() - tm)
    print('EXT: ', datetime.now())
    for tel_ in range(2):
        for fil_ in range(4):
            table_name = 'ExtJohnson' + get_telescope(tel_) + get_filter_band(fil_)
            create_table_ext(table_name)
            print(datetime.now() - tm)
    print("Moon: ", datetime.now())
    create_moon_table()
    print(datetime.now() -tm)
    print("PosX", datetime.now())
    create_position_table()
    print(datetime.now() -tm)

create_tables()
