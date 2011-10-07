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

''' Plot information about providers '''

import getopt
import json
import sys

def main():

    ''' Plot information about providers '''

    json_output = False

    try:
        options, arguments = getopt.getopt(sys.argv[1:], 'J')
    except getopt.error:
        sys.exit('Usage: hist_provider.py [-J] file')
    if len(arguments) != 1:
        sys.exit('Usage: hist_provider.py [-J] file')

    for tpl in options:
        if tpl[0] == '-J':
            json_output = True

    providers = json.load(open(arguments[0], 'r'))
    results = []

    #
    # XXX Assume that the first level is providers and
    # the second level is the test table
    #
    for provider, tables in providers.items():
        stats = tables['speedtest']['uuid']
        results.append((len(set(stats)), len(stats), provider))

    results = sorted(results)

    if json_output:
        json.dump(results, sys.stdout, indent=4, sort_keys=True)
        sys.stdout.write('\n')
    else:
        for neubots, tests, provider in results:
            sys.stdout.write('%s & %d & %d \\\\\n' % (provider, tests,
                                                       neubots))

if __name__ == '__main__':
    main()
