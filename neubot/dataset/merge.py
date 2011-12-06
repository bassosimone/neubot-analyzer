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

''' Merge Neubot databases '''

import atexit
import bz2
import getopt
import os
import sqlite3
import sys
import syslog
import tempfile

sys.path.insert(0, '../neubot')

from neubot.database import DatabaseManager
from neubot.database import migrate

# =======
# sqlite3
# =======

def __sqlite3_cleanup(*args):
    ''' Cleanups temporary files '''
    syslog.syslog(syslog.LOG_INFO, 'Cleanup: %s' % args[0])
    os.unlink(args[0])

def __sqlite3_connect(path):

    '''
     Return a connection to the database at @path.  This function
     takes care of the cases when the database does not exist or
     it is compressed and/or needs to be migrated.
    '''

    # Create new database if nonexistent
    if not os.path.exists(path):
        syslog.syslog(syslog.LOG_INFO, 'Create new: %s' % path)
        manager = DatabaseManager()
        manager.set_path(path)
        connection = manager.connection()
        connection.commit()
        return connection

    # Decompress the database if needed
    if path.endswith('.bz2'):
        inputfp = bz2.BZ2File(path)
        outputfp, npath = tempfile.mkstemp(suffix='.sqlite3', dir='.')
        syslog.syslog(syslog.LOG_INFO, 'Bunzip2: %s -> %s' % (path, npath))
        outputfp = os.fdopen(outputfp, 'w')
        atexit.register(__sqlite3_cleanup, npath)
        chunk = inputfp.read(262144)
        while chunk:
            outputfp.write(chunk)
            chunk = inputfp.read(262144)
        outputfp.close()
        inputfp.close()
    else:
        npath = path

    # Migrate to the latest version
    syslog.syslog(syslog.LOG_INFO, 'Open existing: %s' % npath)
    connection = sqlite3.connect(npath)
    connection.row_factory = sqlite3.Row
    migrate.migrate(connection)
    return connection

# ======
# lookup
# ======

def __lookup_last(connection, table):
    ''' Get the timestamp of the last test '''
    cursor = connection.cursor()
    cursor.execute('SELECT MAX(timestamp) FROM %s;' % table)
    maximum = next(cursor)[0]
    if not maximum:
        return 0
    return maximum

# ==========
# copy table
# ==========

def __construct_query(table, template):
    ''' Create query for table given template '''
    vector = [ 'INSERT INTO %s(' % table ]
    for name in template.keys():
        vector.append(name)
        vector.append(', ')
    vector[-1] = ') VALUES('
    for name in template.keys():
        vector.append(':%s' % name)
        vector.append(', ')
    vector[-1] = ');'
    return ''.join(vector)

def __copy_table(source, destination, table, beginning):
    ''' Copy all the results after @beginning '''
    query = None
    cursor = source.cursor()
    cursor.execute('SELECT * FROM %s WHERE timestamp > ?;'
                   % table, (beginning,))
    count = 0
    for result in cursor:
        result = dict(result)
        # Do NOT copy the original row ID
        del result['id']
        if not query:
            query = __construct_query(table, result)
        destination.execute(query, result)
        count = count + 1
    syslog.syslog(syslog.LOG_INFO, 'Merged %s tuples from %s' % (count, table))
    destination.commit()

# ====
# main
# ====

def main():

    ''' Merge Neubot databases '''

    syslog.openlog('merge.py', syslog.LOG_PERROR, syslog.LOG_USER)
    output = 'database.sqlite3'

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'o:')
    except getopt.error:
        sys.exit('Usage: merge.py [-o output] file...')
    if not arguments:
        sys.exit('Usage: merge.py [-o output] file...')

    for name, value in options:
        if name == '-o':
            output = value

    beginning = {}
    destination = __sqlite3_connect(output)
    for argument in arguments:
        source = __sqlite3_connect(argument)
        for table in ('speedtest', 'bittorrent'):
            # Just in case there are overlapping measurements
            beginning[table] = __lookup_last(destination, table)
            __copy_table(source, destination, table, beginning[table])

    destination.commit()

if __name__ == '__main__':
    main()
