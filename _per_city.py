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

#
# This is experimental code I'm using to produce per-city
# results.
#

import json
import matplotlib.pyplot
import matplotlib.mlab
import pprint
import math
import sys

def __heavy_queue(stats, maximum):
    nstats = []
    for elem in stats:
        if elem > maximum:
            elem = maximum
        nstats.append(elem)
    return nstats

def __per_city(cities, cityname, table, feature, scalefactor, bins,
               cumulative=False, xrange=None, ext='svg'):
    city = cities[cityname]

    if feature in ('dload', 'upload'):
        scaling = lambda value: value * 8.0 / (1000 * 1000)
        xlabel = 'Bulk transfer rate for %s [Mbit/s]' % feature
    elif feature == 'rtt':
        scaling = lambda value: value * 1000.0
        xlabel = 'Time to connect [ms]'
    else:
        raise ValueError('Invalid feature')

    for isp in city.keys():
        if not isp.startswith('AS30722') and not isp.startswith('AS1267') and not isp.startswith('AS12874') and not isp.startswith('AS3269'):
            del city[isp]

    side = int(math.sqrt(len(city))) + 1
    index = 1
    for isp, stats in city.items():
        #matplotlib.pyplot.subplot(side, side, index)
        matplotlib.pyplot.subplot(2, 2, index)
        index = index + 1
        stats = map(scaling, stats[table][feature])

        if xrange:
            stats = __heavy_queue(stats, xrange[1])

        print(isp, len(stats))

        if cumulative:
            matplotlib.pyplot.hist(stats, bins, cumulative=True, range=xrange,
                                   histtype='step', normed=True)
            matplotlib.pyplot.ylim((0, 1.1))
        else:
            matplotlib.pyplot.hist(stats, bins, range=xrange, normed=True)

        matplotlib.pyplot.grid(True, color='black')
        matplotlib.pyplot.title(isp)
        matplotlib.pyplot.ylabel('Frequency')
        matplotlib.pyplot.xlabel(xlabel)

    figure = matplotlib.pyplot.gcf()
    figure.set_figheight(scalefactor * figure.get_figheight())
    figure.set_figwidth(scalefactor * figure.get_figwidth())
    if cumulative:
        figure.savefig('%s-%s-%s_%s.%s' % (cityname, table, feature,
                                           'cumulative', ext))
    else:
        figure.savefig('%s-%s-%s.%s' % (cityname, table, feature, ext))
    figure.clear()

#
# Stuff for the paper.
# Uncomment/comment to decide what to print.
#

filep = open(sys.argv[1], 'rb')
cities = json.load(filep)
filep.close()

#__per_city(cities, 'Turin', 'speedtest', 'rtt', scalefactor=4, bins=200,
#           cumulative=False, xrange=(0, 200), ext='pdf')
__per_city(cities, 'Turin', 'speedtest', 'rtt', scalefactor=4, bins=1000,
           cumulative=True, xrange=(0, 200), ext='pdf')

#__per_city(cities, 'Turin', 'speedtest', 'dload', scalefactor=4, bins=20,
#           cumulative=False, xrange=(0, 20), ext='pdf')
#__per_city(cities, 'Turin', 'speedtest', 'dload', scalefactor=4, bins=1000,
#           cumulative=True, xrange=(0, 20), ext='pdf')

#__per_city(cities, 'Turin', 'speedtest', 'upload', scalefactor=4, bins=20,
#           cumulative=False, xrange=(0, 20), ext='pdf')
#__per_city(cities, 'Turin', 'speedtest', 'upload', scalefactor=4, bins=1000,
#           cumulative=True, xrange=(0, 1), ext='pdf')
