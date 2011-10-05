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

''' Build histograms on Neubot database '''

import GeoIP
import collections
import getopt
import json
import sqlite3
import sys
import syslog

def __open(path):
    ''' Open GeoIP database given the @path '''
    handle = GeoIP.open(path, GeoIP.GEOIP_STANDARD)
    handle.set_charset(GeoIP.GEOIP_CHARSET_UTF8)
    return handle

CITY = __open('/usr/local/share/GeoIP/GeoLiteCity.dat')
ASNAME = __open('/usr/local/share/GeoIP/GeoIPASNum.dat')

def __geolocate(address, facet):

    '''
     Given the @address attempt geolocation and then return
     the selected geolocation @facet (if possible).
    '''

    if facet == 'provider':
        return ASNAME.org_by_addr(address)
    elif facet in ('city', 'country_code'):
        data = CITY.record_by_addr(address)
        if not data:
            return None
        return data[facet]
    else:
        raise RuntimeError('Invalid facet: %s' % facet)

def __build_hist(connection, table, hist, groups):

    '''
     This function walks the @table of the database referenced by
     @connection and builds the @hist.  Depending on the groups
     the result dictionary contains more or less aggregated data.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM %s;' % table)
    for row in cursor:

        row = dict(row)
        stats = hist
        skip = False

        # Honour groups
        for group in groups:
            if group == 'uuid':
                selector = row['uuid']
            elif group in ('provider', 'country_code', 'city'):
                selector = __geolocate(row['real_address'], group)
            else:
                raise RuntimeError('Invalid group: %s' % group)

            if not selector:
                skip = True
                break

            if not selector in stats:
                stats[selector] = {}
            stats = stats[selector]

        # Honour skip
        if skip:
            continue

        # Copy stats
        if not table in stats:
            stats[table] = collections.defaultdict(list)
        for key, value in row.items():
            stats[table][key].append(value)

        # Add window
        for direction in ('download', 'upload'):
            value = row['%s_speed' % direction] * row['connect_time']
            stats[table]['%s_wnd' % direction].append(value)

        # Add speed normalized to 100 ms
        for direction in ('download', 'upload'):
            value = (row['%s_speed' % direction] / row['connect_time']) * 0.1
            stats[table]['%s_norm' % direction].append(value)

USAGE = '''\
Usage: hist_build.py [-d] [-D group] [-o file] file
Groups: city, country_code, provider, uuid'''

def main():

    ''' Info on Neubot database '''

    syslog.openlog('hist_build.py', syslog.LOG_PERROR, syslog.LOG_USER)
    groups = []
    outfp = sys.stdout
    pretty = False

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'D:dno:')
    except getopt.error:
        sys.exit(USAGE)
    if len(arguments) != 1:
        sys.exit(USAGE)

    for name, value in options:
        if name == '-D':
            groups.append(value)
        elif name == '-d':
            pretty = True
        elif name == '-o':
            outfp = open(value, 'w')

    hist = {}
    connection = sqlite3.connect(arguments[0])
    connection.row_factory = sqlite3.Row
    for table in ('speedtest', 'bittorrent'):
        __build_hist(connection, table, hist, groups)

    indent, sort_keys = None, False
    if pretty:
        indent, sort_keys = 4, True
    json.dump(hist, outfp, indent=indent, sort_keys=sort_keys)
    if pretty:
        outfp.write("\n")

if __name__ == '__main__':
    main()
