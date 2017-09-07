#!/usr/bin/env python
"""
Nova Re-pin
Collin May
cmay@mirantis.com
2017-09-02
"""


import sys
from functools import wraps
import logging
from oslo_config import cfg
from nova import config
from nova import service
from nova import objects
from nova import service
from nova import context
from nova.objects import numa
from nova.objects import instance_numa_topology
from nova.api import metadata as instance_metadata
from nova.virt import hardware
from nova.objects import compute_node
from nova import exception
import pinning
#import hardware
from build_new_host_topology import build_new_host_topology

objects.register_all()

CONF = cfg.CONF
CONF.debug = False

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger('pinning')

conf_options = [ cfg.MultiStrOpt('repin', default=None),
                 cfg.MultiStrOpt('unpin', default=None),
                 cfg.MultiStrOpt('pin', default=None),
                 cfg.MultiStrOpt('checkpin', default=None),
                 cfg.BoolOpt('serialize', default=False),
                 cfg.BoolOpt('debug', default=False)]

CONF.register_cli_opts(conf_options, group='pinning')
config.parse_args(sys.argv)



    
def instance_valid(func):
    @wraps(func)
    def validate(inst):
        if type(inst) == objects.instance.Instance:
            valid_inst = inst
        else:
            try:
                ctx = context.get_admin_context()
                ic = objects.instance.Instance()
                valid_inst = ic.get_by_uuid(ctx, inst)
            except Exception as e:
                LOG.error("Instance {} invalid. Raised: {}".format(inst, e))
        LOG.debug("Running {} on {}".format(func.func_name, valid_inst.uuid))
        return func(valid_inst)
    return validate

def pinning_logger(func):
    """
    Add log messages before and after an operation that will change
    an instance's CPU pinning
    """
    @wraps(func)
    def logged(inst):
        LOG.info("Initial Cell Pinning for {}: {}".format(inst.uuid, inst.numa_topology.cells))
        result = func(inst)
        LOG.info("Updated Cell Pinning for {}: {}".format(inst.uuid, inst.numa_topology.cells))
        return result
    return logged

@instance_valid
@pinning_logger
def unpin(instance):
    try:
        for cell in instance.numa_topology.cells:
            cell['cpu_pinning_raw'] = {}
        instance.save()
    except Exception as e:
        LOG.error("Instance UUID: {}".format(instance.uuid))
        LOG.debug("Instance UUID: {} Raised: {}".format(instance.uuid, e))
        
@instance_valid
def checkpin(instance):
    return instance.numa_topology.cells

@instance_valid
@pinning_logger
def repin(instance):
    unpin(instance)
    pin(instance)


@instance_valid
@pinning_logger
def pin(instance):
    nt = numa.NUMATopology()
    computenodelist = objects.compute_node.ComputeNodeList()
    ctx = context.get_admin_context()
    topology_db_object = computenodelist.get_by_hypervisor(ctx, instance.node)[0].numa_topology
    host_topology = nt.obj_from_db_obj(topology_db_object)
    instance_topology = instance.numa_topology
    pinned = pinning.fit_to_host(host_topology, instance_topology)
    #pinned = hardware.numa_fit_instance_to_host(host_topology, instance_topology)
    instance.save()
    return pinned

def main():

    
    if CONF.pinning.debug == True:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)
        
    unpins = CONF.pinning.unpin
    repins = CONF.pinning.repin
    checkpins = CONF.pinning.checkpin
    pins = CONF.pinning.pin
    if unpins:
        for inst in unpins:
            unpin(inst)
    if pins:
        for inst in pins:
            pin(inst)
    if repins:
        for inst in repins:
            repin(inst)
    if checkpins:
        for inst in checkpins:
            r = checkpin(inst)
            print "{}: {}".format(inst, r)

if __name__ == "__main__":
    main()
    print build_new_host_topology('node-3.domain.tld').cells
