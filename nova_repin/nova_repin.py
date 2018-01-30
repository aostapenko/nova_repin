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
import json
import logging

import libvirt
from nova.virt.libvirt import host
from nova import config
from nova import context
from nova import objects
from nova.virt import hardware
import prettytable

objects.register_all()

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger('pinning')

ALLOWED_ACTIONS = ['pin', 'unpin', 'repin']

parser = argparse.ArgumentParser()
parser.add_argument('action', choices=ALLOWED_ACTIONS)
parser.add_argument('instance', type=str)
parser.add_argument('--debug', default=False, action='store_true')
parser.add_argument('--nova-config', default=None)
parser.add_argument('--mysql-connection', default=None)


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
    for cell in instance.numa_topology.cells:
        cell.cpu_pinning = {}


def _pin(instance, compute_node):
    _validate_empty_pinning(instance)
    host_topology = objects.numa.NUMATopology.obj_from_db_obj(
                        compute_node.numa_topology)
    pinned = hardware.numa_fit_instance_to_host(host_topology,
                                                instance.numa_topology)
    instance.numa_topology = pinned
    _update_usage(instance, compute_node, free=False)


def save(f):
    def wrapped(instance, compute_node):
        print_status(instance, compute_node, "Current DB pinning data:")
        f(instance, compute_node)
        print_status(instance, compute_node, "Proposed DB pinning data:")

        if raw_input("Write 'save' to persist proposed data: ") == 'save':
            instance.save()
            compute_node.save()
    return wrapped


@save
def do_unpin(instance, compute_node):
    # NOTE(aostapenko) This will also update numa cpu and memory usage, that is
    # not what we want, however these values will come back on next resource
    # update periodic task
    _unpin(instance, compute_node)


@save
def do_repin(instance, compute_node):
    _unpin(instance, compute_node)
    _pin(instance, compute_node)


@save
def do_pin(instance, compute_node):
    _pin(instance, compute_node)


def _table(fields, values):
    pt = prettytable.PrettyTable(fields)
    pt.align = 'l'
    for row in values:
        pt.add_row(row)
    return pt


def print_status(instance, compute_node, message):
    host_topology = objects.numa.NUMATopology.obj_from_db_obj(
                        compute_node.numa_topology)
    print(message)
    print(_table(("Instance", "Pinning"),
                 ((instance.uuid, instance.numa_topology.cells),)))
    LOG.debug('\n' + str(_table(("Compute Node", "Pinning"),
                                ((instance.host, host_topology.cells),))))
    print("\n\n")


def print_vcpu_pcpu_data(vcpu_pcpu_map):
    vcpu_pcpu_list = []
    for vcpu in sorted(vcpu_pcpu_map.keys()):
        pcpus = [i for i, pcpu_status in enumerate(vcpu_pcpu_map[vcpu])
                 if pcpu_status]
        vcpu_pcpu_list.append((vcpu, pcpus))
    print(_table(("vcpu", "pcpus"), vcpu_pcpu_list))


def calculate_vcpu_pcpu_map(instance, compute_node, total_pcpus):
    host_topology = {
        cell.id: cell for cell in
        objects.numa.NUMATopology.obj_from_db_obj(
            compute_node.numa_topology).cells
    }
    vcpu_pcpu_map = {}
    vcpu_cell_map = {}
    for cell in instance.numa_topology.cells:
        for vcpu in cell.cpuset:
            vcpu_cell_map[vcpu] = cell.id
            if not vcpu_pcpu_map.get(vcpu):
                vcpu_pcpu_map[vcpu] = [False] * total_pcpus
            pinning = cell.cpu_pinning.get(vcpu)
            if pinning is not None:
                vcpu_pcpu_map[vcpu][pinning] = True

    for vcpu, pcpu_list in vcpu_pcpu_map.items():
        if not any(pcpu_list):
            for cpu in host_topology[vcpu_cell_map[vcpu]].cpuset:
                pcpu_list[cpu] = True

    return vcpu_pcpu_map


def apply_to_domain(domain, vcpu_pcpu_map):
    for vcpu, pcpu_list in vcpu_pcpu_map.items():
        domain.pinVcpuFlags(vcpu, tuple(pcpu_list),
                            libvirt.VIR_DOMAIN_AFFECT_LIVE)

def get_vcpu_pcpu_map_from_domain(domain):
    return {i: l for i, l in enumerate(domain.vcpus()[1])}


def main():
    args = parser.parse_args()
    if args.debug:
        LOG.setLevel(logging.DEBUG)

    nova_config = []
    if args.nova_config:
        nova_config = [args.nova_config]
    config.parse_args([], default_config_files=nova_config)
    if args.mysql_connection:
        config.CONF.set_override('connection',
                                 args.mysql_connection,
                                 'database')

    ctx = context.get_admin_context()
    instance = objects.instance.Instance.get_by_uuid(ctx, args.instance)
    compute_node = objects.compute_node.ComputeNode.get_by_host_and_nodename(
        ctx, instance.host, instance.node)

    if args.action not in ALLOWED_ACTIONS:
        raise Exception("Allowed action are: %s" % ', '.join(ALLOWED_ACTIONS))
    action = 'do_{}'.format(args.action)
    eval(action)(instance, compute_node)

    instance = objects.instance.Instance.get_by_uuid(ctx, args.instance)
    compute_node = objects.compute_node.ComputeNode.get_by_host_and_nodename(
        ctx, instance.host, instance.node)
    print_status(instance, compute_node, "Current status:")
    raw_input("Press ENTER to continue")

    # NOTE(aostapenko) handling libvirt verbosity
    def _error_handler(ctx, err):
        pass
    libvirt.registerErrorHandler(_error_handler , None)
    libvirt.virEventRegisterDefaultImpl()

    libvirt_host = host.Host('qemu:///system')
    domain = libvirt_host.get_domain(instance)
    total_pcpus = libvirt_host.get_connection().getInfo()[2]
    vcpu_pcpu_map = calculate_vcpu_pcpu_map(instance, compute_node,
                                            total_pcpus)

    current_vcpu_pcpu_map = get_vcpu_pcpu_map_from_domain(domain)
    print("Current libvirt pinnings:")
    print_vcpu_pcpu_data(current_vcpu_pcpu_map)
    print("Proposed libvirt pinnings, based on current pinning data from DB:")
    print_vcpu_pcpu_data(vcpu_pcpu_map)

    if (raw_input("Write 'apply' to apply proposed pinnings to domain: ") ==
            'apply'):
        apply_to_domain(domain, vcpu_pcpu_map)
        print("Changes were applied to domain")
    else:
        print("Changes were NOT applied to domain")
    print("Current libvirt pinnings:")
    current_vcpu_pcpu_map = get_vcpu_pcpu_map_from_domain(domain)
    print_vcpu_pcpu_data(current_vcpu_pcpu_map)


if __name__ == "__main__":
    main()
