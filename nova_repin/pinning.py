from nova.virt import hardware
import collections
import fractions
import itertools
from nova import objects
from nova import exception
def fit_to_host(
        host_topology, instance_topology, limits=None,
        pci_requests=None, pci_stats=None):
    """Fit the instance topology onto the host topology given the limits

    :param host_topology: objects.NUMATopology object to fit an instance on
    :param instance_topology: objects.InstanceNUMATopology to be fitted
    :param limits: objects.NUMATopologyLimits that defines limits
    :param pci_requests: instance pci_requests
    :param pci_stats: pci_stats for the host

    Given a host and instance topology and optionally limits - this method
    will attempt to fit instance cells onto all permutations of host cells
    by calling the _numa_fit_instance_cell method, and return a new
    InstanceNUMATopology with it's cell ids set to host cell id's of
    the first successful permutation, or None.
    """
    if not (host_topology and instance_topology):
        LOG.debug("Require both a host and instance NUMA topology to "
                  "fit instance on host.")
        return
    else:
        # TODO(ndipanov): We may want to sort permutations differently
        # depending on whether we want packing/spreading over NUMA nodes
        for host_cell_perm in itertools.permutations(
                host_topology.cells, len(instance_topology)):
            cells = []
            for host_cell, instance_cell in zip(
                    host_cell_perm, instance_topology.cells):
                try:
                    got_cell = _numa_fit_instance_cell(
                        host_cell, instance_cell, limits)
                except exception.MemoryPageSizeNotSupported:
                    # This exception will been raised if instance cell's
                    # custom pagesize is not supported with host cell in
                    # _numa_cell_supports_pagesize_request function.
                    break
                if got_cell is None:
                    break
                cells.append(got_cell)
            if len(cells) == len(host_cell_perm):
                if not pci_requests:
                    return objects.InstanceNUMATopology(cells=cells)
                elif ((pci_stats is not None) and
                    pci_stats.support_requests(pci_requests,
                                                     cells)):
                    return objects.InstanceNUMATopology(cells=cells)


def _numa_fit_instance_cell_with_pinning(host_cell, instance_cell):
    """Figure out if cells can be pinned to a host cell and return details

    :param host_cell: objects.NUMACell instance - the host cell that
                      the isntance should be pinned to
    :param instance_cell: objects.InstanceNUMACell instance without any
                          pinning information

    :returns: objects.InstanceNUMACell instance with pinning information,
              or None if instance cannot be pinned to the given host
    """
    if False:
        print "skip usage checks because the instance already lives on this compute node"
    else:
        # Straightforward to pin to available cpus when there is no
        # hyperthreading on the host
        free_cpus = [set([cpu]) for cpu in host_cell.free_cpus]
        print "Host Cell Free CPUs: {}".format(host_cell.free_cpus)
        print "Free CPUs: {}".format(free_cpus)
        return _pack_instance_onto_cores(
            free_cpus, instance_cell, host_cell.id)


def _numa_fit_instance_cell(host_cell, instance_cell, limit_cell=None):
    """Check if an instance cell can fit and set it's cell id

    :param host_cell: host cell to fit the instance cell onto
    :param instance_cell: instance cell we want to fit
    :param limit_cell: an objects.NUMATopologyLimit or None

    Make sure we can fit the instance cell onto a host cell and if so,
    return a new objects.InstanceNUMACell with the id set to that of
    the host, or None if the cell exceeds the limits of the host

    :returns: a new instance cell or None
    """
    # NOTE (ndipanov): do not allow an instance to overcommit against
    # itself on any NUMA cell
    if (instance_cell.memory > host_cell.memory or
            len(instance_cell.cpuset) > len(host_cell.cpuset)):
        return None

    if instance_cell.cpu_pinning_requested:
        print "pinning requested: {}".format(instance_cell.cpu_pinning_requested)
        print "Host Cell: {}\nInstance Cell: {}".format(host_cell, instance_cell)
        new_instance_cell = _numa_fit_instance_cell_with_pinning(
            host_cell, instance_cell)
        if not new_instance_cell:
            return
        new_instance_cell.pagesize = instance_cell.pagesize
        instance_cell = new_instance_cell

    elif limit_cell:
        memory_usage = host_cell.memory_usage + instance_cell.memory
        cpu_usage = host_cell.cpu_usage + len(instance_cell.cpuset)
        cpu_limit = len(host_cell.cpuset) * limit_cell.cpu_allocation_ratio
        ram_limit = host_cell.memory * limit_cell.ram_allocation_ratio
        if memory_usage > ram_limit or cpu_usage > cpu_limit:
            return None

    pagesize = None
    if instance_cell.pagesize:
        pagesize = _numa_cell_supports_pagesize_request(
            host_cell, instance_cell)
        if not pagesize:
            return

    instance_cell.id = host_cell.id
    instance_cell.pagesize = pagesize
    return instance_cell


def _pack_instance_onto_cores(available_siblings, instance_cell, host_cell_id):
    """Pack an instance onto a set of siblings

    :param available_siblings: list of sets of CPU id's - available
                               siblings per core
    :param instance_cell: An instance of objects.InstanceNUMACell describing
                          the pinning requirements of the instance

    :returns: An instance of objects.InstanceNUMACell containing the pinning
              information, and potentially a new topology to be exposed to the
              instance. None if there is no valid way to satisfy the sibling
              requirements for the instance.

    This method will calculate the pinning for the given instance and it's
    topology, making sure that hyperthreads of the instance match up with
    those of the host when the pinning takes effect.
    """

    # We build up a data structure 'can_pack' that answers the question:
    # 'Given the number of threads I want to pack, give me a list of all
    # the available sibling sets that can accommodate it'
    print "Packing instances onto cores"
    can_pack = collections.defaultdict(list)
    for sib in available_siblings:
        print "Sibling: {}".format(sib)
        for threads_no in range(1, len(sib) + 1):
            can_pack[threads_no].append(sib)

    def _can_pack_instance_cell(instance_cell, threads_per_core, cores_list):
        """Determines if instance cell can fit an avail set of cores."""

        if threads_per_core * len(cores_list) < len(instance_cell):
            return False
        if instance_cell.siblings:
            return instance_cell.cpu_topology.threads <= threads_per_core
        else:
            return len(instance_cell) % threads_per_core == 0

    # We iterate over the can_pack dict in descending order of cores that
    # can be packed - an attempt to get even distribution over time
    for cores_per_sib, sib_list in sorted(
            (t for t in can_pack.items()), reverse=True):
        if _can_pack_instance_cell(instance_cell,
                                   cores_per_sib, sib_list):
            sliced_sibs = map(lambda s: list(s)[:cores_per_sib], sib_list)
            if instance_cell.siblings:
                pinning = zip(itertools.chain(*instance_cell.siblings),
                              itertools.chain(*sliced_sibs))
            else:
                pinning = zip(sorted(instance_cell.cpuset),
                              itertools.chain(*sliced_sibs))

            topology = (instance_cell.cpu_topology or
                        objects.VirtCPUTopology(sockets=1,
                                                cores=len(sliced_sibs),
                                                threads=cores_per_sib))
            instance_cell.pin_vcpus(*pinning)
            instance_cell.cpu_topology = topology
            instance_cell.id = host_cell_id
            return instance_cell
