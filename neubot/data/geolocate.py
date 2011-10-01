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

''' Geolocate Neubot database '''

import GeoIP
import getopt
import sqlite3
import sys
import syslog

def __geoip_open(path):
    ''' Open geoip database '''
    handle = GeoIP.open('/usr/local/share/GeoIP/%s' % path,
                        GeoIP.GEOIP_STANDARD)
    handle.set_charset(GeoIP.GEOIP_CHARSET_UTF8)
    return handle

def __geoip_query_org(handle, address):
    ''' Query and return address '''
    return handle.org_by_addr(address)

def __geoip_query_location(handle, address):
    ''' Query and return location '''
    data = handle.record_by_addr(address)
    if data:
        return data['country_code'], data['city']
    else:
        return None, None

def main():

    ''' Geolocate Neubot database '''

    syslog.openlog('geolocate.py', syslog.LOG_PERROR, syslog.LOG_USER)

    try:
        arguments = getopt.getopt(sys.argv[1:], '')[1]
    except getopt.error:
        sys.exit('Usage: geolocate.py file')
    if len(arguments) != 1:
        sys.exit('Usage: geolocate.py file')

    geoip_city = __geoip_open('GeoLiteCity.dat')
    geoip_org = __geoip_open('GeoIPASNum.dat')

    syslog.syslog(syslog.LOG_INFO, 'Geolocate: %s' % arguments[0])
    connection = sqlite3.connect(arguments[0])
    connection.row_factory = sqlite3.Row

    for table in ('speedtest', 'bittorrent'):
        syslog.syslog(syslog.LOG_INFO, 'Table: %s' % table)

        # Add columns
        connection.execute(''' ALTER TABLE %s ADD COLUMN org TEXT;'''
                                    % table)
        connection.execute(''' ALTER TABLE %s ADD COLUMN country_code TEXT;'''
                                    % table)
        connection.execute(''' ALTER TABLE %s ADD COLUMN city TEXT;'''
                                    % table)

        # Walk and add
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM %s' % table)
        for result in cursor:
            org = __geoip_query_org(geoip_org, result['real_address'])
            country_code, city = __geoip_query_location(geoip_city,
              result['real_address'])
            connection.execute(''' UPDATE %s SET org=?, city=?,
              country_code=? WHERE id=?; ''' % table, (org,
              city, country_code, result['id']))

    # Rebuild the database
    connection.execute('VACUUM;')
    connection.commit()

if __name__ == '__main__':
    main()
