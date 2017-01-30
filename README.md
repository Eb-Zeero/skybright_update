# Installation

Download the zip of this repository **do not clone**

Extract the content in the server where the astmon data is located, in the directory of choice

Setup a user personal cron job to run once a day after astmon data had updated
```bash
crontab -e
```
add a line `0 11 * * * username python3 /path/to/update_skybrightness_database.py`

save and exit

*(with the assumption of astmon data will be updated by 11:00)*

**Important note: **The script ha no right to create database database must be created before the script runs.

# Settings

create `configurations.ini` file on a root director

Add the following contentet in side it
```
[MYSQL_CONN]
username = *MySQL-username*
password = *MySQL-password hare*
host = *Database host name*
db = *Database name*
charset = utf8

[LAST_UPDATE]
date_rise = *Initial folder name of sunrise data* #like 20130125
date_set = *Initial folder name of sunset data* #like 20120101

```
The following must also be added to the `configurations.ini` data must be keep as it is.
```

[DATA_PATH]
data = /data/astmon/

[TELESCOPE]
rise = sunrise
set = sunset
rise_name = astmonsunrise
set_name = astmonsunset

[FOLDER]
ext = Extinction
sky = SkyBrightness

```

`configurations.ini` contain setting which are crucial for a smooth run of `update_skybrightness_database.py`

**`MYSQL_CONN`**: `username`, `password`, `host`, `db`(database) must be changed to the real database connection settings
and after that the file must not be in any version control. Make sure that `configuration.ini`
is included in `.gitignore`. The database name must be the same as as in [dbtest](docs/name.md)

**`LAST_UPDATE`**:`date_rise`, `date_set` must be set to 20130101

*(with the assumption of data older than 1st January 2013 is no longer in the server)*

**`DATA_PATH`**: If astmon data directory is changed then the value of `data` must be change to where the astmon data will be located.

**`TELESCOPE`**: If the sunrise data and sunset data directory is renamed or changed then
`rise_name` and `set_name` values must be change to new names.



