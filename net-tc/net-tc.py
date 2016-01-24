#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import re
import subprocess
import shlex

# ...

def net_tc_discovery():

    result = { 'data': [] }

    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:1'})
    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:10'})
    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:20'})
    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:30'})
    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:40'})
    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:50'})
    result['data'].append({'{#IF_NAME}': 'eth1', '{#TC_CLASS_NAME}': '1:60'})

    result['data'].append({'{#IF_NAME}': 'ifb1', '{#TC_CLASS_NAME}': '1:1'})
    result['data'].append({'{#IF_NAME}': 'ifb1', '{#TC_CLASS_NAME}': '1:10'})
    result['data'].append({'{#IF_NAME}': 'ifb1', '{#TC_CLASS_NAME}': '1:20'})
    result['data'].append({'{#IF_NAME}': 'ifb1', '{#TC_CLASS_NAME}': '1:30'})
    result['data'].append({'{#IF_NAME}': 'ifb1', '{#TC_CLASS_NAME}': '1:40'})
    
    print(json.dumps(result))


# ...            

def net_tc_bytes(params):

    params = params.split(',')
    interface = params[0]
    class_name = params[1]

    cmd = "tc -s class show dev " + interface
    proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    response = proc.communicate()[0].decode('utf8')

    lines = response.splitlines()
    i = 0
    current_class = None
    result = {}

    while i < len(lines):

        line = lines[i]
        i = i + 1

        tmp = re.findall(r'class htb ([\w:]*)', line)

        if tmp:
            current_class = tmp[0]
            continue

        tmp = re.findall(r' Sent ([\w:]*) bytes ([\w:]*)', line)

        if tmp and current_class:

            result[current_class] = {}            
            result[current_class]['bytes'] = tmp[0][0]
            result[current_class]['packets'] = tmp[0][1]

    if not class_name in result:
        print('ZBX_NOTSUPPORTED')
        return

    print(result[class_name]['bytes'])
            
# ...

if len(sys.argv) < 0:
    sys.exit(1)

if sys.argv[1] == 'net.tc.discovery':
    net_tc_discovery()
    sys.exit(0)

if sys.argv[1] == 'net.tc.bytes':
    net_tc_bytes(sys.argv[2])
    sys.exit(0)

print('ZBX_NOTSUPPORTED')

