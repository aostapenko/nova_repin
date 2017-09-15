from nova import context
from nova import objects
from nova import exception
from oslo_utils import units
from collections import defaultdict

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
    host_instances = objects.instance.InstanceList().get_by_host(ctx, compute_host)
    instance_topologies = [instance.numa_topology for instance in host_instances if instance.uuid not in excluded_uuids]
    print "Original Host Topology: {}\n".format(host_cpu_usage)
    host_topology = _calculate_cpu_usage(host_topology, instance_topologies)
    print "Updated CPU Usage: {}\n".format(host_cpu_usage)
    host_topology = _calculate_memory_page_usage(host_topology, instance_topologies)
    print "Updated Memory Usage: {}\n".format(host_cpu_usage)
    

def _calculate_cpu_usage(host_topology, instance_topologies=[]):
    """
    Calculate an updated set of used cpus.
    cpu_pinning_raw is a dictionary with the keys being the instance CPU numbers,
    the values are the host's CPU numbers.
    We only need the values from cpu_pinning_raw to calculate this.
    """
    all_inst_cells = [cell for instance in instance_topologies for cell in instance.numa_topology.cells]
    used_cpus = []
    for cell in all_inst_cells:
        used_cpus += cell.cpu_pinning_raw.values()
    used_cpu_set = set(used_cpus)
                
    for cell in host_topology.cells:
        host_available_cpus = cell.cpuset
        """ Take the intersection of the set of avaiable cpus and used cpus to calculate the new pinned_cpus set """
        new_pinned_cpus = host_available_cpus & used_cpu_set
        cell.pinned_cpus = new_pinned_cpus
        cell.cpu_usage = len(new_pinned_cpus)
    return host_topology


def _calculate_memory_usage(host_topology, instance_topologies=[]):
    """
    Calculate the Memory usage for each of the host's numa cells.
    """
    check_for_pages = lambda cell: hasattr(cell, 'mempages')
    all_inst_cells = [cell for instance in instance_topologies for cell in instance.numa_topology.cells]

    reduce(lambda l, r: l and r, map(check_for_pages, all_inst_cells), True)
    
    host_cell_memory_usage = defaultdict(lambda: 0)
    for cell in all_inst_cells:
        host_cell_memory_usage[cell.id] += cell.memory
        
    for cell in host_topology.cells:
        cell.memory_usage = host_memory_cell_usage[cell.id]
        
    return host_topology
    

def _calculate_memory_page_usage(host_topology, instance_topologies=[]):
    """
    Build a dictionary containing the host's numa cells, and a list of instance cells associated the cell
    """
    # host_cells = dict([(x.id,[]) for x in host_topology.cells])
    # host_page_sizes = list(set([page.size_kb for cell in host_topology.cells for page in cell.mempages]))

    all_inst_cells = [cell for instance in instance_topologies for cell in instance.numa_topology.cells]

    host_cells = defaultdict(list)
    for inst_cell in all_inst_cells:
        host_cells[inst_cell.id].append(inst_cell)
    
    consumption = {}
    page_consumption = {}
    for cell, insts in host_cells.items():
        pagesizes = defaultdict(list)
        for inst in insts:
            pagesizes[inst.pagesize].append(inst.memory)
        cell_consumption = defaultdict(lambda: 0)
        cell_page_consumption = defaultdict(lambda: 0)
        for k, v in pagesizes.items():
            # Make sure the instance's ram usage is a multiple of the page size
            if (units.Ki * sum(v)) % k == 0:
                cell_page_consumption[k] = ( sum(v) * units.Ki ) / k
                cell_consumption[k] = sum(v) 
            else:
                raise "Memory usage doesn't line up with page size of {} for cell {}".format(k, cell)
            
        consumption[cell] = cell_consumption
        page_consumption[cell] = cell_page_consumption
    print "Consumption {}".format(consumption)
    print "Page Consumption {}".format(page_consumption)

    # We've been operating on primitives. Update the original host_topology object with our calculations to pass on to the next calculation
    
    for cell in host_topology.cells:
        for page in cell.mempages:
            for page_size in page:
                page_size.used = page_consumption[cell.id][pagesize.size_kb]
        cell.memory_usage = sum(consumption[cell.id].values())
            
    return host_topology
