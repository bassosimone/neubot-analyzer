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

import json
import matplotlib.pyplot
import pprint
import sys

filep = open(sys.argv[1], 'rb')
cities = json.load(filep)
filep.close()
city = cities['Turin']

#
# The piece from now on is quite general and can
# be reused for tool.py
#

feature = 'upload'
table = 'speedtest'

if feature == 'dload':
    scaling = lambda value: value * 8.0 / (1000 * 1000)
    xlabel = 'Measured download bulk transfer rate [Mbit/s]'
    maximum = 100
elif feature == 'upload':
    scaling = lambda value: value * 8.0 / (1000 * 1000)
    xlabel = 'Measured upload bulk transfer rate [Mbit/s]'
    maximum = 100
elif feature == 'rtt':
    scaling = lambda value: value * 1000.0
    xlabel = 'Time to connect [ms]'
    maximum = 500

#
# Sort ISPs from the one with more tests to the one
# with less tests.
#
isps = []
for isp in cities['Turin']:
    isps.append((len(city[isp][table][feature]), isp))
isps = sorted(isps, reverse=True)

for index in range(min(4, len(isps))):
    matplotlib.pyplot.subplot(int("22%d" % (index + 1)))
    isp = isps[index][1]
    stats = city[isp][table][feature]
    stats = map(scaling, stats)
    ranges = None
    if max(stats) > maximum:
        ranges = (0, maximum)
    matplotlib.pyplot.hist(stats, 100, ranges)
    matplotlib.pyplot.title(isp)
    matplotlib.pyplot.xlabel(xlabel)
    matplotlib.pyplot.ylabel('Frequency')

matplotlib.pyplot.show()
