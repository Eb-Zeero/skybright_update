import pymysql
import os

config = {
    'user': os.environ['SKY_DATABASE_USER'],
    'passwd': os.environ['SKY_DATABASE_PASSWORD'],
    'host': os.environ['SKY_DATABASE_HOST'],
    'db': os.environ['SKY_DATABASE_NAME']
}


def find_telescope(tel):
    return "Sunrise" if tel == 0 else "Sunset"


def find_position(pos):
    return 'Zenith' if pos == 0 else 'South' if pos == 1 else 'East' if pos == 2 else 'North' if pos == 3 else 'West'


def _read_data(telescope, position):
    sql = " SELECT DATE_TIME," \
          "     case WHEN FILTER_BAND = 'V'" \
          "         THEN SKYBRIGHTNESS else '-' end 'V_DATA'," \
          "     case WHEN FILTER_BAND = 'B'" \
          "         THEN SKYBRIGHTNESS else '-' end 'B_DATA'," \
          "     case WHEN FILTER_BAND = 'I'" \
          "         THEN SKYBRIGHTNESS else '-' end 'I_DATA'," \
          "     case WHEN FILTER_BAND = 'R' " \
          "         THEN SKYBRIGHTNESS else '-' end 'R_DATA' " \
          " FROM SkyBrightness " \
          " WHERE MOON = 0 AND TELESCOPE = {telescope} AND POSX = {position} AND SKYBRIGHTNESS > 0 " \
          " ORDER BY DATE_TIME".format(telescope=telescope, position=position)

    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(sql)
    data_ = cursor.fetchall()
    return data_


def _write_data(data_, filename):
    file = open(filename, 'a')
    file.write("date\t\t\t\t\t\t| V_data\t\t| B_data\t\t| I_data\t\t| R_data\n"
               "---------------------------------------------------------------------------------------"
               "----------------------\n")
    for line_ in data_:
        v = " -  " if line_[1] == "-" else str("{0: .2f}".format(float(line_[1])))
        b = " -  " if line_[2] == "-" else str("{0: .2f}".format(float(line_[2])))
        i = " -  " if line_[3] == "-" else str("{0: .2f}".format(float(line_[3])))
        r = " -  " if line_[4] == "-" else str("{0: .2f}".format(float(line_[4])))

        file.write(str(line_[0]) + "\t\t\t" +
                   v + "\t\t\t " + b + "\t\t\t " + i + "\t\t\t " + r + "\n")
    file.close()


def create_file():
    for tel_ in range(2):
        for pos_ in range(5):
            filename = find_telescope(tel_) + 'Pos' + str(pos_) + '.txt'
            data = _read_data(tel_, pos_)
            _write_data(data, filename)

create_file()





