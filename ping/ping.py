#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
    really simple python script which returns ping latency from an interface
"""

import subprocess
import shlex
import sys
import re

# ...

if len(sys.argv) != 3:
    print("usage: " + sys.argv[0] + " destination interface")
    sys.exit(0)

destination = sys.argv[1]
interface_name = sys.argv[2]


# ...

cmd = "ifconfig " + interface_name
p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
out = p.communicate()[0].decode('utf8')
retcode = p.returncode

if retcode != 0:
    print('ZBX_NOTSUPPORTED')
    sys.exit(1)

# ...

tmp = re.findall(r' inet addr:([0-9\.]+) ', out)
if len(tmp) != 1:
    print('ZBX_NOTSUPPORTED')
    sys.exit(1)

interface_ip = tmp[0]

# ...

cmd = "ping -c1 -W3 -I " + interface_ip + " " + destination
p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
out = p.communicate()[0].decode('utf8')
retcode = p.returncode

if retcode != 0:
    print('ZBX_NOTSUPPORTED')
    sys.exit(1)

# ...

tmp = re.findall(r'[0-9\.]+/([0-9\.]+)/[0-9\.]+/[0-9\.]+ ms', out)

if len(tmp) != 1:
    print('ZBX_NOTSUPPORTED')
    sys.exit(1)


print(tmp[0])

