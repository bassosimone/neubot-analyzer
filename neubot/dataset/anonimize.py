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

''' Anonimize Neubot database '''

import getopt
import sqlite3
import sys
import syslog

def main():

    ''' Anonimize Neubot database '''

    syslog.openlog('anonimize.py', syslog.LOG_PERROR, syslog.LOG_USER)

    try:
        arguments = getopt.getopt(sys.argv[1:], '')[1]
    except getopt.error:
        sys.exit('Usage: anonimize.py file')
    if len(arguments) != 1:
        sys.exit('Usage: anonimize.py file')

    syslog.syslog(syslog.LOG_INFO, 'Anonimize: %s' % arguments[0])
    connection = sqlite3.connect(arguments[0])
    for table in ('speedtest', 'bittorrent'):
        syslog.syslog(syslog.LOG_INFO, 'Table: %s' % table)
        connection.execute('''UPDATE %s SET internal_address='0.0.0.0',
          real_address='0.0.0.0' WHERE privacy_can_publish != 1;''' % table)

    connection.execute('VACUUM;')
    connection.commit()

if __name__ == '__main__':
    main()
