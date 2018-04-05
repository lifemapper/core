#!/usr/bin/python

import json
import sys
from collections import defaultdict


def mung(data):
    """
    Replace a list of values with a map from the
    non-zero values to the indexes at which they
    occur.
    """
    munged = defaultdict(list)
    for i, datum in enumerate(data):
        if datum != 0:
            munged[datum].append(i)
    return munged

geoJSON = json.load(sys.stdin)

features = []
for feature in geoJSON['features']:
    props = feature['properties']
    props['data'] = mung(props['data'])
    if len(props['data']) > 0:
        features.append(feature)

geoJSON['features'] = features
json.dump(geoJSON, sys.stdout)
