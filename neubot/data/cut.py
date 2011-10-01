#!/usr/bin/python

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

''' Cut Neubot database '''

import calendar
import getopt
import sqlite3
import sys
import syslog
import time

def __mktime(string, fmt):
    ''' Convert string to time '''
    # Force GMT using timegm()
    return int(calendar.timegm(time.strptime(string, fmt)))

USAGE = '''\
Usage: cut.py [-D name=value] file
Macros: city=CITY format=DATE_FMT since=DATE until=DATE'''

def main():

    ''' Cut Neubot database '''

    syslog.openlog('cut.py', syslog.LOG_PERROR, syslog.LOG_USER)
    since, until = 0, int(time.time())
    city = None
    fmt = '%d-%m-%Y'

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'D:')
    except getopt.error:
        sys.exit(USAGE)
    if len(arguments) != 1:
        sys.exit(USAGE)

    for name, value in options:
        if name == '-D':
            name, value = value.split('=', 1)
            if name == 'city':
                city = value
            elif name == 'format':
                fmt = value
            elif name == 'since':
                since = __mktime(value, fmt)
            elif value == 'until':
                until = __mktime(value, fmt)

    connection = sqlite3.connect(arguments[0])
    for table in ('speedtest', 'bittorrent'):
        connection.execute(''' DELETE FROM %s WHERE timestamp < ?
          OR timestamp >= ?; ''' % table, (since, until))
        if city:
            connection.execute(''' DELETE FROM %s WHERE city != '?';'''
                                     % table, (city,))

    connection.execute(' VACUUM; ')
    connection.commit()

if __name__ == '__main__':
    main()
