#!/usr/bin/env python3

import pandas as pd

print("Starting up...")

list = pd.read_csv("sentinel-1.list.gz", header=None, names=["objects"])
list['path'] = list.objects.str[25:58]
counted = list.value_counts(subset=['path']).to_frame('counts')

bad_scenes = counted[counted.counts<8]
bad_scenes.to_csv('missing-data.txt')

print("Done")