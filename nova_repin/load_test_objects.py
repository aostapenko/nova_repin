#!/usr/bin/env python

import os
from nova import objects
import json
from oslo_utils import units
from build_new_host_topology import _calculate_memory_page_usage

objects.register_all()

primitive_dir = os.path.join(os.curdir,'test_primitives')
inst_primitive_files = [os.path.join(primitive_dir, x) for x in os.listdir(primitive_dir) if 'instance-primitive' in x]
host_primitive_files = [os.path.join(primitive_dir, x) for x in os.listdir(primitive_dir) if 'host-primitive' in x]

Inst = objects.Instance()
Host = objects.ComputeNode()

inst_primitives = [json.load(open(x)) for x in inst_primitive_files]
insts = [Inst.obj_from_primitive(x) for x in inst_primitives]

host_primitives = [json.load(open(x)) for x in host_primitive_files]
hosts = [Host.obj_from_primitive(x) for x in host_primitives]

print "insts"
print "hosts"
