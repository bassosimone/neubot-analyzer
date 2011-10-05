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

''' Publish Neubot database '''

import getopt
import sqlite3
import sys
import syslog
import zipfile

def main():

    ''' Publish Neubot database '''

    syslog.openlog('publish.py', syslog.LOG_PERROR, syslog.LOG_USER)
    compress = True

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'n')
    except getopt.error:
        sys.exit('Usage: info.py file')
    if len(arguments) != 1:
        sys.exit('Usage: info.py file')

    for tpl in options:
        if tpl[0] == '-n':
            compress = False

    connection = sqlite3.connect(arguments[0])
    for table in ('speedtest', 'bittorrent'):

        # Check real and internal address
        cursor = connection.cursor()
        cursor.execute('''SELECT COUNT(*) FROM %s WHERE privacy_can_share = 0
          AND (real_address != '0.0.0.0' OR internal_address != '0.0.0.0');'''
           % table)
        count = next(cursor)[0]
        if count > 0:
            raise RuntimeError('Not properly anonymized')

        # Do not disclose bits of maxmind database
        connection.execute('''UPDATE %s SET city='', asname='', country_code=''
           WHERE privacy_can_share != 0;''' % table)

    # Rebuild from scratch
    connection.execute('VACUUM;')
    connection.commit()

    if not compress:
        sys.exit(0)

    dirname = arguments[0].replace('.sqlite3', '')
    zfile = zipfile.ZipFile(dirname + '.zip', 'w', zipfile.ZIP_DEFLATED)
    zfile.write('database-skel/README-txt', '%s/README.txt' % dirname)
    zfile.write('database-skel/LICENSE-txt', '%s/LICENSE.txt' % dirname)
    zfile.write(arguments[0], '%s/database.sqlite3' % dirname)
    zfile.close()

if __name__ == '__main__':
    main()
