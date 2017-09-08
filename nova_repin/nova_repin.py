#!/usr/bin/env python
"""
Nova Re-pin
Collin May
cmay@mirantis.com
2017-09-02
"""

import prettytable
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
from pinning import fit_to_host

objects.register_all()

CONF = cfg.CONF
CONF.debug = False

logging.basicConfig(level=logging.INFO)
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
        valid_inst = None
        if type(inst) == objects.instance.Instance:
            valid_inst = inst
        else:
            try:
                ctx = context.get_admin_context()
                ic = objects.instance.Instance()
                valid_inst = ic.get_by_uuid(ctx, inst)
            except Exception as e:
                LOG.warn("{}".format(e))
            if valid_inst:
                try:
                    valid_inst.numa_topology.cells
                except:
                    LOG.warn("Instance {} has no pinning information".format(inst))
                    valid_inst = None

        if valid_inst:
            LOG.debug("Valid Instance '{}' Running {}".format(valid_inst.uuid, func.func_name))
            return func(valid_inst)
        else:
            return
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
def unpin(instance):
    try:
        for cell in instance.numa_topology.cells:
            cell['cpu_pinning_raw'] = {}
        instance.save()
    except Exception as e:
        LOG.error("Error Unpinning Instance: {}".format(instance.uuid))
        LOG.debug("Instance UUID: {} Raised: {}".format(instance.uuid, e))
        
@instance_valid
def checkpin(instance):
    print "Pinning Information for {}: {}".format(instance.uuid, instance.numa_topology.cells)
    return instance.numa_topology.cells

@instance_valid
def repin(instance):
    # we are passing the same instance object into the pin function
    # pin will take care of saving if it successfully generates a new mapping
    old_topology = instance.numa_topology
    LOG.debug("Instance Topology for {} before repinning: {}".format(instance.uuid, old_topology))
    LOG.debug("Instance Pinning {} before repinning: {}".format(instance.uuid, old_topology.cells))

    for cell in instance.numa_topology.cells:
        cell['cpu_pinning_raw'] = {}
    pin(instance)


@instance_valid
def pin(instance):
    nt = numa.NUMATopology()
    computenodelist = objects.compute_node.ComputeNodeList()
    ctx = context.get_admin_context()
    topology_db_object = computenodelist.get_by_hypervisor(ctx, instance.node)[0].numa_topology
    host_topology = build_new_host_topology(instance.host, instance.uuid)
    LOG.debug("Host Topology: {}".format(host_topology.cells))

    instance_topology = instance.numa_topology
    old_topology = instance_topology.obj_clone()
    LOG.debug("Old Instance Topology for {}: {}".format(instance.uuid, old_topology))
    LOG.debug("Old Instance Pinning {}: {}".format(instance.uuid, old_topology.cells))

    pinned = fit_to_host(host_topology, instance_topology)
    LOG.debug("New Instance Topology for {}: {}".format(instance.uuid, pinned))
    LOG.debug("New Instance Pinning {}: {}".format(instance.uuid, instance.numa_topology.cells))

    """
    # Test that saves don't overwrite with an empty mapping
    for cell in instance.numa_topology.cells:
        cell['cpu_pinning_raw'] = {}
    """
    
    cells_are_populated = [cell.cpu_pinning_raw != {} for cell in instance.numa_topology.cells]
    if reduce(lambda l, r: l or r, cells_are_populated):
        LOG.debug("Saving new pinning for {}".format(instance.uuid))
        instance.save()
    else:
        LOG.warn("Pinning for {} failed".format(instance.uuid))
    return {'old': old_topology, 'new': pinned}

def checkpins_table(checkpins):
    validate = instance_valid(lambda x: x)
    pt = prettytable.PrettyTable(['Instance', 'Pinning'])
    pt.align = 'l'
    # pt.header = False
    for inst in checkpins:
        res = validate(inst)
        if res:
            res_cells = str(res.numa_topology.cells)
        else:
            res_cells = "Not Found"
        pt.add_row([inst, res_cells])
    # print pt.get_string()
    return pt

def main():

    
    if CONF.pinning.debug == True:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)
        
    unpins = CONF.pinning.unpin
    repins = CONF.pinning.repin
    checkpins = CONF.pinning.checkpin
    pins = CONF.pinning.pin
    all_pins = []
    if unpins:
        all_pins += unpins
        unpins = set(unpins)
        check_unpins = checkpins_table(unpins)
        print "Before Unpinning\n{}\n".format(check_unpins)
        for inst in unpins:
            unpin(inst)

    if pins:
        all_pins += pins
        pins = set(pins)
        check_pins = checkpins_table(pins)
        print "Before Pinning\n{}\n".format(check_pins)
        for inst in set(pins):
            pin(inst)
            
    if repins:
        all_pins += repins
        repins = set(repins)
        check_repins = checkpins_table(repins)
        print "Before Repinning\n{}\n".format(check_repins)

        for inst in set(repins):
            repin(inst)
            
    if checkpins:
        all_pins += checkpins


    all_checkpins = checkpins_table(set(all_pins))
    print "Current Pinning Information\n{}\n".format(all_checkpins)

if __name__ == "__main__":
    main()
