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

'''
 This is the script I use to manage and analyze the set of Neubot
 databases collected over time.  This is just for personal use and
 so might not work for you out of the box.
'''

import collections
import decimal
import getopt
import json
import syslog
import sqlite3
import time
import bz2
import re
import sys
import os

from matplotlib import pyplot
from matplotlib import dates

sys.path.insert(0, '../neubot')

from neubot.database import DATABASE
from neubot.database import migrate

class __FakeGeoIP:
    ''' Fake geoip provider '''

    def record_by_addr(self, address):
        ''' Fake record_by_addr method '''

    def org_by_addr(self, address):
        ''' Fake org_by_addr method '''

def __get_geoloc_city():
    ''' Return a geoloc city provider '''
    try:
        import GeoIP
        return GeoIP.open('/usr/local/share/GeoIP/GeoLiteCity.dat',
                          GeoIP.GEOIP_STANDARD)
    except:
        return __FakeGeoIP()

def __get_geoloc_asn():
    ''' Return a geoloc asn provider '''
    try:
        import GeoIP
        return GeoIP.open('/usr/local/share/GeoIP/GeoIPASNum.dat',
                          GeoIP.GEOIP_STANDARD)
    except:
        return __FakeGeoIP()

GEOLOC_CITY = __get_geoloc_city()
GEOLOC_ASN = __get_geoloc_asn()

def __connect(path):

    '''
     This function connects to the database at @path and sets up
     sqlite3.Row as row factory, so that we can treat results both
     as tuples and as dictionaries.
    '''

    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection

def __info(connection):

    '''
     This function reads information on the database referenced
     by @connection, i.e. reads and prints ``config`` table.
    '''

    result = {}

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM config;')
    for name, value in cursor:
        result[name] = value

    return result

def __decompress(path):

    '''
     This function decompress a compressed Neubot database
     on the current working directory
    '''

    outputpath = os.path.basename(path).replace('.bz2', '')

    inputfp = bz2.BZ2File(path)
    outputfp = open(outputpath, 'w')
    while True:
        chunk = inputfp.read(262144)
        if not chunk:
            break
        outputfp.write(chunk)
    outputfp.close()
    inputfp.close()

    return outputpath

def __create_empty(path):

    '''
     This functions creates an empty Neubot database at @path,
     jusing Neubot internals to do that.
    '''

    DATABASE.set_path(path)
    connection = DATABASE.connection()
    connection.commit()
    connection.close()

def __migrate(connection):

    '''
     This function migrates the database at @connection to the
     current database format.  To do that, this function uses
     directly the Neubot routines written for this purpose.
    '''

    migrate.migrate(connection)

def __sanitize(table):

    '''
     Write query making sure that the table name contains only
     lowercase letters or underscores.
    '''

    stripped = re.sub(r'[^a-z_]', '', table)
    if stripped != table:
        raise RuntimeError("Invalid table name")

    return table

def __lookup_can_share(connection, table):

    '''
     This function lookups the number of measurement with the
     can_share permission in @table in the database referenced
     by @connection.
    '''

    cursor = connection.cursor()
    cursor.execute('''SELECT COUNT(timestamp) FROM %s
      WHERE privacy_can_share=1;''' % __sanitize(table))
    count = next(cursor)[0]
    if not count:
        return 0
    return count

def __lookup_count_uuids(connection, table):

    '''
     This function lookups the number of uuids in @table in the
     database referenced by @connection.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(DISTINCT(uuid)) FROM %s;' % __sanitize(table))
    count = next(cursor)[0]
    if not count:
        return 0
    return count

def __lookup_count(connection, table):

    '''
     This function lookups the number of measurement in @table in the
     database referenced by @connection.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(timestamp) FROM %s;' % __sanitize(table))
    count = next(cursor)[0]
    if not count:
        return 0
    return count

def __lookup_first(connection, table):

    '''
     This function lookups the first measurement in @table in the
     database referenced by @connection.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT MIN(timestamp) FROM %s;' % __sanitize(table))
    minimum = next(cursor)[0]
    if not minimum:
        return 0
    return minimum

def __lookup_last(connection, table):

    '''
     This function lookups the last measurement in @table in the
     database referenced by @connection.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT MAX(timestamp) FROM %s;' % __sanitize(table))
    maximum = next(cursor)[0]
    if not maximum:
        return 0
    return maximum

def __copyto_after(source, destination, table, limit):

    '''
     Copy from @source to @destination the content of @table
     which has timestamp greater than @limit.
    '''

    query = None

    cursor = source.cursor()
    cursor.execute('SELECT * FROM %s WHERE timestamp > ?;'
                   % __sanitize(table), (limit,))
    for result in cursor:
        dictionary = dict(result)

        # Otherwise we overwrite our data
        del dictionary['id']

        # Construct the query
        if not query:
            vector = ['INSERT INTO %s(' % __sanitize(table) ]
            for name in dictionary.keys():
                vector.append(name)
                vector.append(', ')
            vector[-1] = ') VALUES('
            for name in dictionary.keys():
                vector.append(':%s' % name)
                vector.append(', ')
            vector[-1] = ');'

            query = "".join(vector)

        # Insert
        destination.execute(query, dictionary)

    # Save
    destination.commit()

def __anonimize(connection, table):

    ''' Anonimize @table of the database referenced by @connection '''

    # Add columns
    connection.execute(''' ALTER TABLE %s ADD COLUMN asname TEXT;'''
                                % __sanitize(table))
    connection.execute(''' ALTER TABLE %s ADD COLUMN country_code TEXT;'''
                                % __sanitize(table))
    connection.execute(''' ALTER TABLE %s ADD COLUMN city TEXT;'''
                                % __sanitize(table))

    # Gather location and provider info
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM %s' % __sanitize(table))
    for row in cursor:

        # Avoid violating MaxMind copyright
        if int(row['privacy_can_share']):
            continue

        # Ditch user address
        connection.execute('''UPDATE %s SET internal_address="0.0.0.0",
          real_address="0.0.0.0" WHERE id=?;''' %
          __sanitize(table), (row['id'],))

        # Provider information
        asname = GEOLOC_ASN.org_by_addr(row['real_address'])
        if asname:
            asname = asname.decode('latin-1')
            connection.execute(''' UPDATE %s SET asname=?
              WHERE id=?''' % __sanitize(table), (asname, row['id']))

        # Geo information
        geodata = GEOLOC_CITY.record_by_addr(row['real_address'])
        if geodata:
            if geodata['country_code']:
                country_code = geodata['country_code'].decode('latin-1')
                connection.execute(''' UPDATE %s SET country_code=?
                  WHERE id=?''' % __sanitize(table), (country_code, row['id']))
            if geodata['city']:
                city = geodata['city'].decode('latin-1')
                connection.execute(''' UPDATE %s SET city=?
                  WHERE id=?''' % __sanitize(table), (city, row['id']))

    # Rebuild the database
    connection.execute('VACUUM')
    connection.commit()

def __format_date(thedate):
    ''' Make a timestamp much more readable '''
    return time.ctime(int(thedate))

def __build_histogram(connection, table, histogram, modifiers):

    '''
     This function walks the @table of the database referenced by
     @connection and collects statistics.  Depending on the params
     the result dictionary contains more or less aggregated data.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM %s' % __sanitize(table))
    for row in cursor:

        stats = histogram
        skip = False

        for modifier in modifiers:

            if modifier == 'per_instance':
                instance = row['uuid']
                if not instance:
                    skip = True
                    break

                if not instance in stats:
                    stats[instance] = {}

                stats = stats[instance]

            elif modifier == 'per_provider':
                provider = GEOLOC_ASN.org_by_addr(row['real_address'])
                if not provider:
                    skip = True
                    break

                # Avoid issues with provider name
                provider = provider.decode('latin-1')

                if not provider in stats:
                    stats[provider] = {}

                stats = stats[provider]

            elif modifier == 'per_country':
                geodata = GEOLOC_CITY.record_by_addr(row['real_address'])
                if not geodata or not geodata['country_code']:
                    skip = True
                    break

                # Avoid issues with country code
                country = geodata['country_code'].decode('latin-1')

                if not country in stats:
                    stats[country] = {}

                stats = stats[country]

            elif modifier == 'per_city':
                geodata = GEOLOC_CITY.record_by_addr(row['real_address'])
                if not geodata or not geodata['city']:
                    skip = True
                    break

                # Avoid issues with city name
                city = geodata['city'].decode('latin-1')

                if not city in stats:
                    stats[city] = {}

                stats = stats[city]

            elif modifier == 'per_hour':

                #
                # FIXME The problem with the hour calculator
                # below is that it does not take into account
                # the time zone.  Since we're interested in
                # Italy at the moment and we're in summer we
                # optimize for CEST.
                #
                hour = (((row['timestamp']/3600) + 2) % 24)

                if not hour in stats:
                    stats[hour] = {}

                stats = stats[hour]

            else:
                raise RuntimeError('Invalid modifier: %s' % modifier)

        if skip:
            continue

        #
        # Save stats
        #
        if not stats:
            stats.update({
                            'bittorrent':
                              {
                                'dload': [],
                                'upload': [],
                                'rtt': [],
                              },
                            'speedtest':
                              {
                                'dload': [],
                                'upload': [],
                                'rtt': [],
                              },
                            'first_test': 0,
                            'last_test': 0,
                          })

        # First and last test info
        if not stats['first_test']:
            stats['first_test'] = row['timestamp']
        if row['timestamp'] > stats['last_test']:
            stats['last_test'] = row['timestamp']

        stats[table]['dload'].append(row['download_speed'])
        stats[table]['upload'].append(row['upload_speed'])
        stats[table]['rtt'].append(row['connect_time'])

def main():

    ''' Dispatch control to various subcommands '''

    syslog.openlog('neubot [tool]', syslog.LOG_PERROR, syslog.LOG_USER)

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'AMHiflNo:TX:')
    except getopt.error:
        sys.exit('Usage: tool.py -AMHiNT [-fl] [-o output] [-X modifier] input ...')

    outfile = 'database.sqlite3'
    modifiers = []

    flag_histogram = False
    flag_anonimize = False
    flag_merge = False
    flag_pretty = False
    flag_info = False
    flag_force = False
    flag_number = False
    flag_tests = False

    for name, value in options:

        if name == '-A':
            flag_anonimize = True
        elif name == '-M':
            flag_merge = True
        elif name == '-i':
            flag_info = True
        elif name == '-H':
            flag_histogram = True
        elif name == '-N':
            flag_number = True
        elif name == '-T':
            flag_tests = True

        elif name == '-X':
            modifiers.append(value)

        elif name == '-f':
            flag_force = True
        elif name == '-l':
            flag_pretty = True

        elif name == '-o':
            outfile = value

    sum_all = flag_anonimize + flag_merge + flag_info + flag_histogram + \
              flag_number + flag_tests

    if sum_all > 1:
        sys.exit('Only one of -AMHiNT may be specified')
    if sum_all == 0:
        sys.exit('Usage: tool.py -AMHiNT [-fl] [-o output] [-X modifier] input ...')

    #
    # Collate takes a set of (possibly compressed) databases
    # and appends to the output database only the set of results
    # that is missing.  So, it is safe to invoke it each time
    # against the same set of files.
    # Note that merge will skip old databases unless the
    # force parameter is True.
    #
    if flag_merge:

        if not os.path.exists(outfile):
            __create_empty(outfile)
        elif not os.path.isfile(outfile):
            raise RuntimeError('Not a file')

        destination = __connect(outfile)
        __migrate(destination)

        for argument in arguments:

            # Decompress if needed
            if argument.endswith('.bz2'):
                syslog.syslog(syslog.LOG_INFO, 'decompress %s' % argument)
                argument = __decompress(argument)

            # Query configuration
            source = __connect(argument)
            info = __info(source)
            version = decimal.Decimal(info['version'])

            # Skip old databases
            if version <= decimal.Decimal('2.0') and not flag_force:
                syslog.syslog(syslog.LOG_WARNING, 'skipping old %s' % argument)
                continue

            # Migrate
            syslog.syslog(syslog.LOG_INFO, 'migrate %s' % argument)
            __migrate(source)

            # Save
            for table in ('speedtest', 'bittorrent'):
                syslog.syslog(syslog.LOG_INFO, 'merging table %s' % table)
                limit = __lookup_last(destination, table)
                __copyto_after(source, destination, table, limit)

    #
    # Print information on the database so that one can get
    # an idea of the information contained.
    #
    elif flag_info:

        for argument in arguments:
            target = __connect(argument)
            __migrate(target)

            dictionary = __info(target)
            dictionary['filename'] = argument
            for table in ('speedtest', 'bittorrent'):
                dictionary[table] = {}
                dictionary[table]['count_uuids'] = __lookup_count_uuids(target,
                                                                        table)
                dictionary[table]['count'] = __lookup_count(target, table)
                dictionary[table]['can_share'] = __lookup_can_share(target,
                                                                    table)
                first = __lookup_first(target, table)
                last = __lookup_last(target, table)

                if flag_pretty:
                    first = __format_date(first)
                    last = __format_date(last)

                dictionary[table]['first'] = first
                dictionary[table]['last'] = last

            indent = None
            if flag_pretty:
                indent = 4

            json.dump(dictionary, sys.stdout, indent=indent)

            if flag_pretty:
                sys.stdout.write("\n")

    #
    # Zap all internet addresses with no can_share permission
    # attached regardless of the other settings.
    #
    elif flag_anonimize:

        for argument in arguments:
            target = __connect(argument)
            __migrate(target)
            __anonimize(target, 'speedtest')
            __anonimize(target, 'bittorrent')

    #
    # Walk the database and collect statistics where the
    # aggregation level depends on command line options
    #
    elif flag_histogram:

        histogram = {}
        for argument in arguments:
            target = __connect(argument)
            __migrate(target)
            for table in ('speedtest', 'bittorrent'):
                __build_histogram(target, table, histogram, modifiers)

        sort_keys, indent = False, None
        if flag_pretty:
            sort_keys, indent = True, 4

        json.dump(histogram, sys.stdout, indent=indent, sort_keys=sort_keys)

        if flag_pretty:
            sys.stdout.write("\n")

    #
    # Compute the cumulated number of active agents at a
    # given time period.
    #
    elif flag_number:

        number_of_agents = collections.defaultdict(set)

        for argument in arguments:
            target = __connect(argument)
            __migrate(target)
            for table in ('speedtest', 'bittorrent'):
                cursor = target.cursor()
                cursor.execute('SELECT * FROM %s;' % __sanitize(table))
                for row in cursor:
                    when = int(dates.epoch2num(row['timestamp']))
                    number_of_agents[when].add(row['uuid'])

        cumulated, xdata, ydata = 0, [], []
        for when in sorted(number_of_agents.keys()):
            xdata.append(when)
            ydata.append(len(number_of_agents[when]))

        pyplot.plot_date(xdata, ydata)
        pyplot.show()

    # Tries to count the number of tests per day.
    elif flag_tests:

        xdata, ydata = [], []
        helper = collections.defaultdict(int)

        for argument in arguments:
            target = __connect(argument)
            __migrate(target)
            for table in ('speedtest', 'bittorrent'):
                cursor = target.cursor()
                cursor.execute('SELECT * FROM %s;' % __sanitize(table))
                for row in cursor:
                    when = int(dates.epoch2num(row['timestamp']))
                    helper[when] += 1

        for when in sorted(helper.keys()):
            xdata.append(when)
            ydata.append(helper[when])

        pyplot.plot_date(xdata, ydata)
        pyplot.show()

if __name__ == '__main__':
    main()
