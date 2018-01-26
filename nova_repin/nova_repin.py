#!/usr/bin/env python
#
# Nova Re-pin
# Collin May
# cmay@mirantis.com
# 2017-09-02
# 
# Andrey Ostapenko
# aostapenko@mirantis.com
# 2018-01-25
#

import argparse
import functools
import logging
import sys

from nova import config
from nova import context
from nova import objects
from nova.virt import hardware
import prettytable

objects.register_all()

# NOTE(aostapenko) Allow nova config to set default arguments, not using it
# for script purposes
config.parse_args([])

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger('pinning')

ALLOWED_ACTIONS = ['pin', 'unpin', 'repin']

parser = argparse.ArgumentParser()
parser.add_argument('action', choices=ALLOWED_ACTIONS)
parser.add_argument('instance', type=str)
parser.add_argument('--debug', default=False, action='store_true')


def _update_usage(instance, compute_node, free):
    updated_numa_topology = hardware.get_host_numa_usage_from_instance(
        compute_node, instance, free)
    compute_node.numa_topology = updated_numa_topology


def _validate_empty_pinning(instance):
    cells = instance.numa_topology.cells
    for cell in cells:
        if cell.cpu_pinning:
            raise Exception("Can't pin already pinned instance. "
                            "Unpin it first. (%s)" % cells)


def _unpin(instance, compute_node):
    _update_usage(instance, compute_node, free=True)
    # NOTE (aostapenko) Not using clear_host_pinning to keep compatibility with
    # vanilla kilo
    for cell in instance.numa_topology.cells:
        cell.id = -1
        cell.cpu_pinning = {}


def _pin(instance, compute_node):
    _validate_empty_pinning(instance)
    host_topology = objects.numa.NUMATopology.obj_from_db_obj(
                        compute_node.numa_topology)
    pinned = hardware.numa_fit_instance_to_host(host_topology,
                                                instance.numa_topology)
    instance.numa_topology = pinned
    _update_usage(instance, compute_node, free=False)


def do_unpin(instance, compute_node):
    _unpin(instance, compute_node)
    instance.save()
    compute_node.save()


def do_repin(instance, compute_node):
    _unpin(instance, compute_node)
    _pin(instance, compute_node)
    instance.save()
    compute_node.save()


def do_pin(instance, compute_node):
    _pin(instance, compute_node)
    instance.save()
    compute_node.save()


def _table(fields, values):
    pt = prettytable.PrettyTable(fields)
    pt.align = 'l'
    pt.add_row(values)
    return pt


def print_status(instance, compute_node, message):
    host_topology = objects.numa.NUMATopology.obj_from_db_obj(
                        compute_node.numa_topology)
    print(message)
    print(_table(("Instance", "Pinning"),
                (instance.uuid, instance.numa_topology.cells)))
    print(_table(("Compute Node", "Pinning"),
                (instance.host, host_topology.cells)))
    print "\n\n"


def main():
    args = parser.parse_args()
    if args.debug:
        LOG.setLevel(logging.DEBUG)

    ctx = context.get_admin_context()
    instance = objects.instance.Instance.get_by_uuid(ctx, args.instance)
    compute_node = objects.compute_node.ComputeNode.get_by_host_and_nodename(
        ctx, instance.host, instance.node)

    print_status(instance, compute_node, "Before %s:" % args.action)
    action = 'do_{}'.format(args.action)
    eval(action)(instance, compute_node)
    print_status(instance, compute_node, "After %s:" % args.action)

    instance = objects.instance.Instance.get_by_uuid(ctx, args.instance)
    compute_node = objects.compute_node.ComputeNode.get_by_host_and_nodename(
        ctx, instance.host, instance.node)
    print_status(instance, compute_node, "Current status:")


if __name__ == "__main__":
    main()
