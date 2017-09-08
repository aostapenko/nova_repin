from nova import context
from nova import objects
from nova import exception

"""
Take a set X of CPUs in the hostsnuma cell, take a set Y of CPUs used for all numa cells exclusing the instances we're unpinning.

Find the union of the two sets for each host numa cell. This will give us the available CPUs for each numa cell.

Create a new numa_topology based off of the hypervisor's numa_topology, but with the updated  pinned_cpus values

Pass this new numa_topology object into the fit instance to host function
"""



def build_new_host_topology(compute_host, excluded_instance_uuids=[]):
    """
    Build a fresh set of CPUs availe for pinning
    Exclude CPUs used by instances we want to unpin
    """
    ctx = context.get_admin_context()
    nt = objects.numa.NUMATopology()

    """
    ComputeNodeList gives us our CPU topology as a DB object
    not something we can operate on directly. We need to build a NUMATopology object from the
    database json we were given
    """
    compute_node = objects.compute_node.ComputeNodeList().get_by_hypervisor(ctx,compute_host)[0]
    host_topology = nt.obj_from_db_obj(compute_node.numa_topology)

    """
    Calculate an updated set of used cpus.
    cpu_pinning_raw is a dictionary with the keys being the instance CPU numbers,
    the values are the host's CPU numbers.
    We only need the values from cpu_pinning_raw to calculate this.
    """
    host_instances = objects.instance.InstanceList().get_by_host(ctx, compute_host)
    used_cpus = []
    for inst in host_instances:
        if inst.uuid not in excluded_instance_uuids:
            for cell in inst.numa_topology.cells:
                used_cpus += cell.cpu_pinning_raw.values()
    used_cpu_set = set(used_cpus)
                
    for cell in host_topology.cells:
        host_available_cpus = cell.cpuset
        """ Take the intersection of the set of avaiable cpus and used cpus to calculate the new pinned_cpus set """
        new_pinned_cpus = host_available_cpus & used_cpu_set
        cell.pinned_cpus = new_pinned_cpus
    return host_topology
    

