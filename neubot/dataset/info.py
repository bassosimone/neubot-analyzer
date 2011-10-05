#!/usr/bin/env python

#
# Copyright (c) 2011 Simone Basso <bassosimone@gmail.com>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

''' Info on Neubot database '''

import getopt
import json
import sqlite3
import sys
import syslog
import time

def __info_config(connection):
    ''' Returns config table content information '''
    dictionary = {}
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM config;')
    for name, value in cursor:
        dictionary[name] = value
    return dictionary

def __info_uuids(connection, table):
    ''' How many unique uuids in table '''
    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(DISTINCT(uuid)) FROM %s;' % table)
    count = next(cursor)[0]
    if not count:
        return 0
    return count

def __info_tests(connection, table):
    ''' How many tests in table '''
    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM %s;' % table)
    count = next(cursor)[0]
    if not count:
        return 0
    return count

def __info_publishable(connection, table):
    ''' How many publishable tests '''
    cursor = connection.cursor()
    cursor.execute('''SELECT COUNT(*) FROM %s
      WHERE privacy_can_share=1;''' % table)
    count = next(cursor)[0]
    if not count:
        return 0
    return count

def __info_geolocated(connection, table):
    ''' Tells whether the database is geolocated '''
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM %s LIMIT 1;' % table)
    for description in cursor.description:
        if description[0] == 'city':
            return True
    return False

def __info_anonymized(connection, table):
    ''' Tells whether the database is anonimized '''
    cursor = connection.cursor()
    cursor.execute('''SELECT COUNT(*) FROM %s WHERE privacy_can_share = 0
      AND (real_address != '0.0.0.0' OR internal_address != '0.0.0.0');'''
      % table)
    count = next(cursor)[0]
    return count == 0

def __info_test_first(connection, table):
    ''' Timestamp of first test '''
    cursor = connection.cursor()
    cursor.execute('SELECT MIN(timestamp) FROM %s;' % table)
    minimum = next(cursor)[0]
    if not minimum:
        return 0
    return minimum

def __info_test_last(connection, table):
    ''' Timestamp of last test '''
    cursor = connection.cursor()
    cursor.execute('SELECT MAX(timestamp) FROM %s;' % table)
    maximum = next(cursor)[0]
    if not maximum:
        return 0
    return maximum

def __format_date(thedate):
    ''' Make a timestamp much more readable '''
    return time.ctime(int(thedate))

def main():

    ''' Info on Neubot database '''

    syslog.openlog('info.py', syslog.LOG_PERROR, syslog.LOG_USER)
    outfp = sys.stdout
    pretty = False

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'do:')
    except getopt.error:
        sys.exit('Usage: info.py file')
    if len(arguments) != 1:
        sys.exit('Usage: info.py file')

    for name, value in options:
        if name == '-d':
            pretty = True
        elif name == '-o':
            outfp = open(value, 'w')

    connection = sqlite3.connect(arguments[0])

    dictionary = __info_config(connection)
    dictionary['filename'] = arguments[0]

    for table in ('speedtest', 'bittorrent'):
        dictionary[table] = {}
        dictionary[table]['count_uuids'] = __info_uuids(connection, table)
        dictionary[table]['count_tests'] = __info_tests(connection, table)
        dictionary[table]['count_tests_publishable'] = \
                                          __info_publishable(connection, table)
        dictionary[table]['geolocated'] = __info_geolocated(connection, table)
        dictionary[table]['anonymized'] = __info_anonymized(connection, table)

        first = __info_test_first(connection, table)
        last = __info_test_last(connection, table)

        if pretty:
            first = __format_date(first)
            last = __format_date(last)

        dictionary[table]['first'] = first
        dictionary[table]['last'] = last

    indent, sort_keys = None, False
    if pretty:
        indent, sort_keys = 4, True
    json.dump(dictionary, outfp, indent=indent, sort_keys=sort_keys)
    if pretty:
        outfp.write("\n")

if __name__ == '__main__':
    main()
