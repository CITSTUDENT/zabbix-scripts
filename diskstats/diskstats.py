#!/usr/bin/python

import sys
import re
import json

DISKSTATS_PATH = '/proc/diskstats'
HEADERS = ['major_number', 'minor_number', 'device_name',
           'reads_requests_completed', 'reads_requests_merged', 'reads_sectors', 'reads_wait_time',
           'writes_requests_completed', 'writes_requests_merged', 'writes_sectors', 'writes_wait_time',
           'requests_currently_in_progress', 'io_wait_time', 'io_wait_time_weighted']

# ...

key = sys.argv[1]

# ...

def parse_diskstats():

    result = {}

    with open(DISKSTATS_PATH, 'r') as f:

        for line in f.readlines():

            tmp = dict(zip(HEADERS, re.split('\s+', line.strip())))

            tmp['reads_bytes'] = str(int(tmp['reads_sectors']) * 512)
            tmp['writes_bytes'] = str(int(tmp['writes_sectors']) * 512)

            result[tmp['device_name']] = tmp            

    return result

# ...

if key == 'diskstats.discovery':

    result = {'data': []}

    diskstats = parse_diskstats()
    
    for name in diskstats:

        tmp = diskstats[name]

        item = {'{#DISK_NAME}': tmp['device_name'],
                '{#DISK_MAJOR}': tmp['major_number'],
                '{#DISK_MINOR}': tmp['minor_number']}

        result['data'].append(item)

    print(json.dumps(result))

    sys.exit(0)

# ...

if key == 'diskstats.item':

    disk = sys.argv[2]
    item = sys.argv[3]

    diskstats = parse_diskstats()

    if disk not in diskstats:
        sys.exit(1)
    
    if item not in diskstats[disk]:
        sys.exit(2)

    print(diskstats[disk][item])

    sys.exit(0)

# ...

sys.exit(1)
