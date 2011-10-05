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
import pylab
import sqlite3
import sys
import syslog

def __open(path):
    ''' Open GeoIP database given the @path '''
    handle = GeoIP.open(path, GeoIP.GEOIP_STANDARD)
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

def __load_table(connection, table, providers):

    ''' Load from database table '''

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM %s;' % table)
    for row in cursor:
        line = collections.defaultdict(list)
        line.update(row)

        # Locate uuid and provider
        uuid = line['uuid']
        provider = __geolocate(line['real_address'], 'provider')
        if not uuid or not provider:
            continue
        provider = provider.decode('latin-1').split()[0]

        # Add window
        for direction in ('download', 'upload'):
            value = line['%s_speed' % direction] * line['connect_time']
            line['%s_wnd' % direction] = value

        # Add speed normalized to 100 ms
        for direction in ('download', 'upload'):
            value = (line['%s_speed' % direction] / line['connect_time']) * 0.1
            line['%s_norm' % direction] = value

        # Remove unneeded fields
        del line['id'], line['internal_address'], line['neubot_version'], \
          line['platform'], line['privacy_can_collect'], \
          line['privacy_informed'], line['privacy_can_share'], \
          line['uuid'], line['timestamp'], line['remote_address'], \
          line['real_address']

        # Locate per-neubot per-provider stats
        if not provider in providers:
            providers[provider] = {}
        if not uuid in providers[provider]:
            providers[provider][uuid] = collections.defaultdict(list)
        stats = providers[provider][uuid]

        # Save stats
        for name, value in line.items():
            stats[name].append(value)

def __plot_download_speed(providers, names, minimum, maximum):

    ''' Plot download speed cumulative distribution '''

    for name in names:
        provider = providers[name]
        hist = []
        for uuid, stats in provider.items():
            maxval = max(stats['download_speed']) * 8e-06
            if maxval < minimum or maxval >= maximum:
                continue
            for result in stats['download_speed']:
                hist.append(result * 8e-06)

        pylab.grid(True, color='black')
        pylab.hist(hist, bins=100000, cumulative=True, normed=True,
                   histtype='step', label=name)

    legend = pylab.legend()
    frame = legend.get_frame()
    frame.set_alpha(0.25)

    pylab.figure(2)
    for name in names:
        provider = providers[name]
        hist = []
        for uuid, stats in provider.items():
            maxval = max(stats['download_speed']) * 8e-06
            if maxval < minimum or maxval >= maximum:
                continue
            for result in stats['download_wnd']:
                hist.append(result)

        pylab.grid(True, color='black')
        pylab.hist(hist, bins=100000, cumulative=True, normed=True,
                   histtype='step', label=name)

    legend = pylab.legend()
    frame = legend.get_frame()
    frame.set_alpha(0.25)

USAGE = 'Usage: hist.py [-dJ] [-o file] file'

def main():

    ''' Info on Neubot database '''

    syslog.openlog('hist.py', syslog.LOG_PERROR, syslog.LOG_USER)

    groups = []
    fromjson = False
    outfile = None
    pretty = False

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'dJo:')
    except getopt.error:
        sys.exit(USAGE)
    if len(arguments) != 1:
        sys.exit(USAGE)

    for name, value in options:
        if name == '-d':
            pretty = True
        elif name == '-J':
            fromjson = True
        elif name == '-o':
            outfile = value

    syslog.syslog(syslog.LOG_INFO, 'Loading database')

    if fromjson:
        providers = json.load(open(arguments[0], 'r'))
    else:
        providers = {}
        connection = sqlite3.connect(arguments[0])
        connection.row_factory = sqlite3.Row
        for table in ('speedtest', 'bittorrent'):
            __load_table(connection, table, providers)

    syslog.syslog(syslog.LOG_INFO, 'Database loaded')

    if outfile:
        if outfile == '-':
            outfp = sys.stdout
        else:
            outfp = open(outfile, 'w')
        if pretty:
            json.dump(providers, outfp, indent=4, sort_keys=True)
            outfp.write("\n")
        else:
            json.dump(providers, outfp)
        sys.exit(0)

    __plot_download_speed(providers, [
                                      #'AS30722',
                                      #'AS1267',
                                      'AS12874',
                                      #'AS3269',
                                     ], 4, 7)
    pylab.show()

if __name__ == '__main__':
    main()
