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

''' Counts number of users/tests per day '''

import collections
import getopt
import pylab
import sqlite3
import sys
import syslog

def main():

    ''' Counts number of users/tests per day '''

    syslog.openlog('count.py', syslog.LOG_PERROR, syslog.LOG_USER)
    count_users = False
    outfile = None

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'o:u')
    except getopt.error:
        sys.exit('Usage: count.py [-o file] [-u] file')
    if len(arguments) != 1:
        sys.exit('Usage: count.py [-o file] [-u] file')

    for name, value in options:
        if name == '-o':
            outfile = value
        elif name == '-u':
            count_users = True

    connection = sqlite3.connect(arguments[0])
    data = collections.defaultdict(list)
    for table in ('speedtest', 'bittorrent'):
        cursor = connection.cursor()
        cursor.execute('SELECT timestamp, uuid FROM %s;' % table)
        for result in cursor:
            timestamp = int(pylab.epoch2num(result[0]))
            data[timestamp].append(result[1])
    connection.close()

    xdata, ydata = [], []
    for timestamp in sorted(data.keys()):
        xdata.append(timestamp)
        uuid_list = data[timestamp]
        if count_users:
            uuid_list = set(uuid_list)
        ydata.append(len(uuid_list))

    result = pylab.plot_date(xdata, ydata)
    pylab.grid(True, color='black')
    pylab.xlabel('Date', fontsize=16)
    if count_users:
        pylab.suptitle('Number of neubots per day', fontsize=20)
        pylab.ylabel('Number of neubots', fontsize=16)
    else:
        pylab.suptitle('Number of tests per day', fontsize=20)
        pylab.ylabel('Number of tests', fontsize=16)

    # Pretty dates
    pylab.gcf().autofmt_xdate()

    if outfile:
        pylab.savefig(outfile, dpi=256, transparent=True)
    else:
        pylab.show()

if __name__ == '__main__':
    main()
