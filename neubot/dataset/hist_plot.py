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

''' Plot histograms '''

import getopt
import json
import pylab
import sys

USAGE = '''
Usage: hist_plot.py [-CENS] [-D selection] [-F scaling-factor]
                    [-L lower-bound] [-n bins] [-o file] [-T title]
                    [-U upper-bound] [-X label] [-Y label] file

Options:
    -C                  : cumulative mode
    -D selection        : select only this facet
    -E                  : exclude out of bounds
    -F scaling-factor   : scaling factor
    -L lower-bound      : distribution lower-bound
    -N                  : normed mode
    -n bins             : number of bins
    -o file             : output file
    -S                  : step histogram mode
    -T title            : title
    -U upper-bound      : distribution upper-bound
    -X label            : X axis label
    -Y label            : Y axis label
'''

def main():

    ''' Info on Neubot database '''

    selections = []
    scalingfactor = None
    lowerbound = None
    outfile = None
    bins = 10
    normed = False
    cumulative = False
    histtype = 'bar'
    upperbound = None
    exclude = False
    xlabel = ''
    title = ''
    ylabel = ''

    try:
        options, arguments = getopt.getopt(sys.argv[1:],
                                 'CED:F:L:Nn:o:ST:U:X:Y:')
    except getopt.error:
        sys.exit(USAGE)
    if len(arguments) != 1:
        sys.exit(USAGE)

    for name, value in options:
        if name == '-C':
            cumulative = True
        elif name == '-D':
            selections.append(value)
        elif name == '-E':
            exclude = True
        elif name == '-F':
            if value == 'Mbit/s':
                scalingfactor = 8.0/(1000 * 1000)
            elif value == 'Kbit/s':
                scalingfactor = 8.0/(1000)
            elif value == 'ms':
                scalingfactor = 1000.0
            elif value == 'KBytes':
                scalingfactor = 1.0/1024
            else:
                scalingfactor = float(value)
        elif name == '-L':
            lowerbound = float(value)
        elif name == '-N':
            normed = True
        elif name == '-n':
            bins = int(value)
        elif name == '-o':
            outfile = value
        elif name == '-S':
            histtype = 'step'
        elif name == '-T':
            title = value
        elif name == '-U':
            upperbound = float(value)
        elif name == '-X':
            xlabel = value
        elif name == '-Y':
            ylabel = value

    ohist = json.load(open(arguments[0], 'r'))
    for selection in selections:
        hist = ohist
        for facet in selection.split('/')[1:]:
            hist = hist[facet]

        nhist = []
        for elem in hist:
            if scalingfactor != None:
                elem = elem * scalingfactor
            if lowerbound != None and elem < lowerbound:
                if exclude:
                    continue
                elem = lowerbound
            if upperbound != None and elem > upperbound:
                if exclude:
                    continue
                elem = upperbound
            nhist.append(elem)
        hist = nhist

        label = selection.split('/')[1]

        pylab.grid(True, color='black')
        pylab.hist(hist, bins=bins, cumulative=cumulative, normed=normed,
                   histtype=histtype, label=label)

    pylab.xlabel(xlabel, fontsize=16)
    pylab.ylabel(ylabel, fontsize=16)
    pylab.title(title, fontsize=20)

    legend = pylab.legend(loc=4)
    frame = legend.get_frame()
    frame.set_alpha(0.25)

    if outfile:
        pylab.savefig(outfile, dpi=256, transparent=True)
    else:
        pylab.show()

if __name__ == '__main__':
    main()
