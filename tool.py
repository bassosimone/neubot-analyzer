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
import GeoIP
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

sys.path.insert(0, '/home/simone/git/neubot')

from neubot.database import DATABASE
from neubot.database import migrate

GEOLOC_CITY = GeoIP.open('/usr/local/share/GeoIP/GeoLiteCity.dat',
                         GeoIP.GEOIP_STANDARD)

GEOLOC_ASN = GeoIP.open('/usr/local/share/GeoIP/GeoIPASNum.dat',
                         GeoIP.GEOIP_STANDARD)

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
     This function lookups the number of measurement in @table in the
     database referenced by @connection.
    '''

    cursor = connection.cursor()
    cursor.execute('''SELECT COUNT(timestamp) FROM %s
      WHERE privacy_can_share=1;''' % __sanitize(table))
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

    connection.execute('''UPDATE %s SET internal_address="0.0.0.0",
                          real_address="0.0.0.0" WHERE
                          privacy_can_share != 1;''' % table)
    connection.commit()
    connection.execute('VACUUM')
    connection.commit()

def __format_date(thedate):
    ''' Make a timestamp much more readable '''
    return time.ctime(int(thedate))

def __follow_instances(connection, table, instances):

    '''
     This function walks the @table of the database referenced by
     @connection and per Neubot instance statistics.  It saves the
     results in @instances.
    '''

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM %s' % __sanitize(table))
    for result in cursor:

        # Get instance ID
        instanceid = result['uuid']
        if not instanceid:
            continue

        # Create per-instance stats
        if not instanceid in instances:
            instances[instanceid] = {}

        instance = instances[instanceid]

        # Create per-provider system stats
        organization = GEOLOC_ASN.org_by_addr(result['real_address'])
        if not organization:
            continue

        if not organization in instance:
            instance[organization] = \
              {
                'bittorrent':
                  {
                    'dload': collections.defaultdict(int),
                    'upload': collections.defaultdict(int),
                    'rtt': collections.defaultdict(int),
                  },
                'speedtest':
                  {
                    'dload': collections.defaultdict(int),
                    'upload': collections.defaultdict(int),
                    'rtt': collections.defaultdict(int),
                  },
                'addresses': collections.defaultdict(int),
                'countries': collections.defaultdict(int),
                'cities': collections.defaultdict(int),
              }

        provider = instance[organization]

        # Fill per-provider stats
        provider['addresses'][result['real_address']] += 1

        geodata = GEOLOC_CITY.record_by_addr(result['real_address'])
        if geodata:
            if geodata['country_code']:
                provider['countries'][geodata['country_code'].decode('latin-1')] += 1
            if geodata['city']:
                provider['cities'][geodata['city'].decode('latin-1')] += 1

        stats = provider[table]

        # Update download (bytes/s -> megabit/s)
        scaled = int(round(result['download_speed'] / 125000))
        stats['dload'][scaled] += 1

        # Update upload (bytes/s -> megabit/s)
        scaled = int(round(result['upload_speed'] / 125000))
        stats['upload'][scaled] += 1

        # Update RTT (seconds -> milliseconds)
        scaled = int(round(result['connect_time'] * 100)) * 10
        stats['rtt'][scaled] += 1

def main():

    ''' Dispatch control to various subcommands '''

    syslog.openlog('neubot [tool]', syslog.LOG_PERROR, syslog.LOG_USER)

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'AMIiflo:P')
    except getopt.error:
        sys.exit('Usage: tool.py -AMIiP [-fl] [-o output] input ...')

    outfile = 'database.sqlite3'
    flag_per_provider = False
    flag_anonimize = False
    flag_merge = False
    flag_pretty = False
    flag_info = False
    flag_instances = False
    flag_force = False

    for name, value in options:

        if name == '-A':
            flag_anonimize = True
        elif name == '-M':
            flag_merge = True
        elif name == '-i':
            flag_info = True
        elif name == '-I':
            flag_instances = True
        elif name == '-P':
            flag_per_provider = True

        elif name == '-f':
            flag_force = True
        elif name == '-l':
            flag_pretty = True

        elif name == '-o':
            outfile = value

    sum_all = flag_anonimize + flag_merge + flag_info + flag_instances \
                  + flag_per_provider

    if sum_all > 1:
        sys.exit('Only one of -AMIiP may be specified')
    if sum_all == 0:
        sys.exit('Usage: tool.py -AMIiP [-fl] [-o output] input ...')

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
    # Walk the database and collect statistics per each
    # existing instance of Neubot
    #
    elif flag_instances:

        instances = {}
        for argument in arguments:
            target = __connect(argument)
            __migrate(target)

            __follow_instances(target, 'speedtest', instances)
            __follow_instances(target, 'bittorrent', instances)

        sort_keys, indent = False, None
        if flag_pretty:
            sort_keys, indent = True, 4

        json.dump(instances, sys.stdout, indent=indent, sort_keys=sort_keys)

        if flag_pretty:
            sys.stdout.write("\n")

    #
    # Create per-instance statistics and then postprocess
    # it to extract statistics per-provider.
    #
    elif flag_per_provider:

        instances = {}
        for argument in arguments:
            target = __connect(argument)
            __migrate(target)

            __follow_instances(target, 'speedtest', instances)
            __follow_instances(target, 'bittorrent', instances)

        providers = {}

        for instance in instances.values():
            for provider_name, instance_stats in instance.iteritems():

                # Create per-provider statistics
                if not provider_name in providers:
                    providers[provider_name] = \
                      {
                        'bittorrent':
                          {
                            'dload': collections.defaultdict(int),
                            'upload': collections.defaultdict(int),
                            'rtt': collections.defaultdict(int),
                          },
                        'speedtest':
                          {
                            'dload': collections.defaultdict(int),
                            'upload': collections.defaultdict(int),
                            'rtt': collections.defaultdict(int),
                          },
                        'addresses': collections.defaultdict(int),
                        'countries': collections.defaultdict(int),
                        'cities': collections.defaultdict(int),
                      }

                provider = providers[provider_name]

                # Copy addresses, countries, cities
                for table in ('addresses', 'countries', 'cities'):
                    for key, value in instance_stats[table].iteritems():
                        provider[table][key] += value

                # Copy bittorrent, speedtest
                for table in ('bittorrent', 'speedtest'):
                    for feature in ('dload', 'upload', 'rtt'):
                        for key, value in instance_stats[table][
                                        feature].iteritems():
                            provider[table][feature][key] += value

        sort_keys, indent = False, None
        if flag_pretty:
            sort_keys, indent = True, 4

        json.dump(providers, sys.stdout, indent=indent, sort_keys=sort_keys)

        if flag_pretty:
            sys.stdout.write("\n")

if __name__ == '__main__':
    main()
