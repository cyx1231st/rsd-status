from __future__ import print_function

import json
from pprint import pprint
import requests

import rsd_lib
import sushy
from sushy import exceptions as sushy_exceptions

import print_utils as utils


rsd_url = "http://http://10.1.0.99:30000"
username = "admin"
password = "admin"
verify = False
GIGABYTE = 1024*1024*1024

def init():
    requests.urllib3.disable_warnings()
    rsd = rsd_lib.RSDLib(rsd_url, username, password, verify=verify).factory()
    return rsd


### NODE ###

# def create_node(rsd):
#     node_col = rsd.get_node_collection()
#     node = node_col.compose_node(
#       name='testnode',
#       description='this is a node',
#       processor_req=[{
#           'TotalCores': 4
#       }],
#       memory_req=[{
#           'CapacityMiB': 8000,
#           'MemoryDeviceType': 'DDR'
#       }],
#       remote_drive_req=[{
#           'CapacityGiB': 80,
#           'iSCSIAddress': 'iqn.oem.com:42',
#           'Master': {
#               'Type': 'Snapshot',
#               'Resource': '/redfish/v1/Services/1/LogicalDrives/1'
#           }
#       }]
#     )
#     # node = node_col.compose_node()
#     node_inst = rsd.get_node(node)
#     return node_inst

def get_node_by_uuid(rsd, uuid):
    uuid = uuid.upper()
    nodes = rsd.get_node_collection().get_members()
    for node in nodes:
        if node and node.system\
                and node.system.uuid\
                and node.system.uuid.upper() == uuid:
            return node
    return None

def get_nodes(rsd):
    node_col = rsd.get_node_collection()
    nodes = node_col.get_members()
    return nodes

def get_node(rsd, path):
    return rsd.get_node(path)

def show_node(rsd, node):
    assert isinstance(node, rsd_lib.resources.v2_3.node.Node)
    utils.print_header("Node", node)
    utils.print_body(node, (
        "uuid",
        "power_state",
        "composed_node_state"))
    print("allowed boot sources: %s" %
        ", ".join(node.get_allowed_node_boot_source_values()))
    print("allowed reset vals: %s" %
        ", ".join(node.get_allowed_reset_node_values()))
    print("allowed detach endp:")
    vols = []
    eps = []
    others = []
    for url in node.get_allowed_detach_endpoints():
        if "Endpoints" in url:
            eps.append(url)
        elif "Volumes" in url:
            vols.append(url)
        else:
            others.append(url)
    vols = (get_volume(rsd, url) for url in vols)
    show_volumes(rsd, vols, 1)
    eps = (get_endpoint(rsd, url) for url in eps)
    show_endpoints(rsd, eps, 1)
    for o in others:
        print("  !! %s" % o)
    print("allowed attach endp:")
    volumes = [url for url in node.get_allowed_attach_endpoints()]
    for vol in volumes:
        print("  %s" % vol)
    # volumes = [get_volume(rsd, url) for url in volumes]
    # show_volumes(rsd, volumes, 1)
    print("links:")
    links = node.links
    assert 6 == len(links.keys())
    print("  system: %s" % links.system)
    print("  processors: %s" % len(links.processors))
    print("  memories: %s" % len(links.memory))
    print("  interfaces: %s" % len(links.ethernet_interfaces))
    print("  local drives: %s" % len(links.local_drives))
    print("  remote drives: %s" % len(links.remote_drives))
    #NOTE: JSON
    links = node.json["Links"]
    assert(7 == len(links))
    print("  oem: %s" % links["Oem"])
    print("  managers:")
    for v in links["ManagedBy"]:
        #TODO
        print("    %s" % v["@odata.id"])
    print("  storages:")
    vols = []
    dvs = []
    others = []
    for url in links["Storage"]:
        url = url["@odata.id"]
        if "Drives" in url:
            dvs.append(url)
        elif "Volumes" in url:
            vols.append(url)
        else:
            others.append(url)
    vols = (get_volume(rsd, url) for url in vols)
    show_volumes(rsd, vols, 2)
    dvs = (get_drive(rsd, url) for url in dvs)
    show_drives(rsd, dvs, 2)
    for o in others:
        print("    !! %s" % o)
    utils.print_body_none(node, (
        "memory_summary",
        "processor_summary"))
    if node.system:
        system = get_system(rsd, node.system.path)
        show_system(rsd, system)

def show_nodes(rsd, nodes, level=0):
    for node in nodes:
        assert isinstance(node, rsd_lib.resources.v2_3.node.Node)
        links_ = node.json["Links"]

        try:
            system = get_system(rsd, node.system.path)
            processors = system.processors.get_members()
            interfaces = system.ethernet_interfaces.get_members()
            macs = ",".join(i.mac_address[-6:] for i in interfaces)
            links = system.json["Links"]
        except Exception as e:
            system = None
            links = None

        try:
            len_attach_endpoints = len(node.get_allowed_attach_endpoints())
        except Exception as e:
            len_attach_endpoints = 0

        print("%s%s \"%s\": %s %s; "
              "dv(l/r) %s/%s; eps %s/%s/%s; mgrs %s/%s; sts %s" % (
            "  "*level,
            node.identity,
            node.name,
            node.status.state,
            node.composed_node_state,
            len(node.links.local_drives),
            len(node.links.remote_drives),
            len(node.get_allowed_detach_endpoints()),
            len_attach_endpoints,
            len(links['Endpoints']) if links else 0,
            len(links_["ManagedBy"]),
            len(links["ManagedBy"]) if links else 0,
            len(links_["Storage"])), end="")

        if system:
            print(" | %s %s ~%s %s %s, proc %s/%s/%s, "
                  "mem %sGiB, eth %s" % (
                      system.identity,
                      system.status.state,
                      node.uuid.split("-")[-1],
                      system.system_type,
                      system.power_state,
                      len(processors),
                      sum(p.total_cores for p in processors),
                      sum(p.total_threads for p in processors),
                      system.memory_summary.size_gib,
                      macs))
        else:
            print("")
    if not level:
        print()


### SYSTEM ###

def get_systems(rsd):
    return rsd.get_system_collection().get_members()

def get_system(rsd, path):
    assert("Systems" in path)
    assert(path.count("/") >= 4)
    sys_path = "/".join(path.split("/", 5)[:5])
    system = rsd.get_system(sys_path)
    return system

def show_system(rsd, system):
    assert isinstance(system,
            rsd_lib.resources.v2_2.system.system.System)
    utils.print_header("System", system)
    utils.print_body(system, (
        'uuid',
        'system_type',
        'power_state',
        'bios_version',
        'manufacturer',
        'serial_number',
        'part_number',
        'sku'))
    print("allowed reset vals: %s" %
            ",".join(system.get_allowed_reset_system_values()))
    print("allowed boot vals: %s" %
            ",".join(system.get_allowed_system_boot_source_values()))
    metrics = system.metrics
    print("metrics: health %s, io %sGbps, mem/cpu %s/%s%%, "
            "mem/cpu %s/%sW, mem throttle %s%%" % (
            ",".join(metrics.health),
            metrics.io_bandwidth_gbps,
            metrics.memory_bandwidth_percent,
            metrics.processor_bandwidth_percent,
            metrics.memory_power_watt,
            metrics.processor_power_watt,
            metrics.memory_throttled_cycles_percent))
    ps = system.json["ProcessorSummary"]
    print("processor_summary: %s; %s; %s" % (
        ps["Count"], ps["Status"]["State"], ps["Model"]))
    print("processors:")
    show_processors(rsd, system.processors.get_members(), 1)
    utils.print_items(system, "memory_summary")
    print("memories:")
    show_memories(rsd, system.memory.get_members(), 1)
    print("interfaces:")
    show_interfaces(rsd, system.ethernet_interfaces.get_members(), 1)
    print("oem: %s" % ", ".join(system.json["Oem"].keys()))
    print("PCIeDevices: %s" % len(system.json["PCIeDevices"]))
    print("PCIeFunctions: %s" % len(system.json["PCIeFunctions"]))
    print("SimpleStorage: %s" % system.json["SimpleStorage"]["@odata.id"])
    print("Storage: %s" % system.json["Storage"]["@odata.id"])
    print("links:")
    links = system.json["Links"]
    assert(5 == len(links))
    print("  Chassises:")
    chassises = [v["@odata.id"] for v in links["Chassis"]]
    chassises = [get_chassis(rsd, url) for url in chassises]
    show_chassises(rsd, chassises, 2)
    print("  Endpoints:")
    endpoints = [v["@odata.id"] for v in links["Endpoints"]]
    endpoints = [get_endpoint(rsd, url) for url in endpoints]
    show_endpoints(rsd, endpoints, 2)
    print("  Managers:")
    for v in links["ManagedBy"]:
        # TODO
        print("    %s" % v["@odata.id"])
    print("  Oem: %s" % links["Oem"])
    utils.print_body_none(system, (
        'asset_tag',
        'hostname',
        'indicator_led'))
    print()

def show_systems(rsd, systems, level=0):
    for system in systems:
        assert isinstance(system,
                rsd_lib.resources.v2_2.system.system.System)
        processors = system.processors.get_members()
        interfaces = system.ethernet_interfaces.get_members()
        macs = ",".join(i.mac_address[-6:] for i in interfaces)
        links = system.json["Links"]
        print("%s%s \"%s\": %s ~%s %s %s, proc %s/%s/%s, "
              "mem %sGiB(%s), eth %s, eps %s, chs %s, mgrs %s, PCIes %s" % (
                  "  "*level,
                  system.identity,
                  system.name,
                  system.status.state,
                  system.uuid.split("-")[-1],
                  system.system_type,
                  system.power_state,
                  len(processors),
                  sum(p.total_cores for p in processors),
                  sum(p.total_threads for p in processors),
                  system.memory_summary.size_gib,
                  len(system.memory.members_identities),
                  macs,
                  len(links['Endpoints']),
                  len(links["Chassis"]),
                  len(links["ManagedBy"]),
                  len(system.json["PCIeDevices"])))
    if not level:
        print()


### SYSTEM/INTERFACE ###

def get_interfaces(rsd):
    systems = get_systems(rsd)
    interfaces = []
    for system in systems:
        interfaces.extend(system.ethernet_interfaces.get_members())
    return interfaces

def get_interface(rsd, path):
    system = get_system(rsd, path)
    assert("EthernetInterfaces" in path)
    assert(path.count("/") == 6)
    interface = system.ethernet_interfaces.get_member(path)
    return interface

def show_interface(rsd, interface):
    assert(isinstance(interface,
        sushy.resources.system.ethernet_interface.EthernetInterface))
    utils.print_header("Interface", interface)
    utils.print_body(interface, (
        'mac_address',
        'permanent_mac_address',
        'speed_mbps'))
    print("VLANs: %s" % interface.json["VLANs"]["@odata.id"])
    print()

def show_interfaces(rsd, interfaces, level=0):
    for interface in interfaces:
        assert(isinstance(interface,
            sushy.resources.system.ethernet_interface.EthernetInterface))
        print("%s%s \"%s\": %s, %s Mbps, %s" % (
            "  "*level,
            interface.identity,
            interface.name,
            interface.status.state,
            interface.speed_mbps,
            interface.mac_address))
    if not level:
        print()


### SYSTEM/PROCESSOR ###

def get_processors(rsd):
    systems = get_systems(rsd)
    processors = []
    for system in systems:
        processors.extend(system.processors.get_members())
    return processors

def get_processor(rsd, path):
    system = get_system(rsd, path)
    assert("Processors" in path)
    assert(path.count("/") == 6)
    processor = system.processors.get_member(path)
    return processor

def show_processor(rsd, processor):
    assert(isinstance(processor,
        rsd_lib.resources.v2_2.system.processor.Processor))
    utils.print_header("Processor", processor)
    utils.print_body(processor, (
        'processor_type',
        'total_cores',
        'total_threads',
        'max_speed_mhz',
        'socket',
        'instruction_set',
        'manufacturer',
        'model'))
    metrics = processor.metrics
    print("metrics: %s; health %s; %sW, %s/%s'C, avg %sMHz" % (
        metrics.identity,
        ",".join(metrics.health),
        metrics.consumed_power_watt,
        metrics.temperature_celsius,
        metrics.throttling_celsius,
        metrics.average_frequency_mhz))
    utils.print_items(processor, "processor_id")
    utils.print_body_none(processor, (
        'processor_architecture',))
    print()

def show_processors(rsd, processors, level=0):
    for processor in processors:
        assert(isinstance(processor,
            rsd_lib.resources.v2_2.system.processor.Processor))
        print("%s%s: %s %s %s %sMHz, %s/%s core/tds, health %s, %s'C, %sW, %s" % (
            "  "*level,
            processor.identity,
            processor.status.state,
            processor.processor_type,
            processor.socket,
            processor.max_speed_mhz,
            processor.total_cores,
            processor.total_threads,
            ",".join(processor.metrics.health),
            processor.metrics.temperature_celsius,
            processor.metrics.consumed_power_watt,
            processor.instruction_set))
    if not level:
        print()


### SYSTEM/MEMORY ###

def get_memories(rsd):
    systems = get_systems(rsd)
    memories = []
    for system in systems:
        memories.extend(system.memory.get_members())
    return memories

def get_memory(rsd, path):
    system = get_system(rsd, path)
    assert("Memory" in path)
    assert(path.count("/") == 6)
    memory = system.memory.get_member(path)
    return memory

def show_memory(rsd, memory):
    assert isinstance(memory,
            rsd_lib.resources.v2_2.system.memory.Memory)
    utils.print_header("Memory", memory)
    utils.print_body(memory, (
        'capacity_mib',
        'operating_speed_mhz',
        'allowed_speeds_mhz',
        'bus_width_bits',
        'data_width_bits',
        'memory_device_type',
        'manufacturer',
        'device_locator',
        'error_correction',
        'part_number',
        'rank_count',
        'redfish_version',
        'serial_number'))
    utils.print_items(memory, "memory_location")
    print("metrics: %s; health %s; %s'C" % (
        memory.metrics.identity,
        ",".join(memory.metrics.health),
        memory.metrics.temperature_celsius))
    utils.print_body_none(memory, (
        'base_module_type',
        'device_id',
        'firmware_revision',
        'frirmware_api_version',
        'function_classes',
        'max_tdp_milliwatts',
        'memory_media',
        'memory_type',
        'operating_memory_modes',
        'vendor_id'))
    print()

def show_memories(rsd, memories, level=0):
    for memory in memories:
        assert isinstance(memory,
                rsd_lib.resources.v2_2.system.memory.Memory)
        print("%s%s \"%s\": %s %sMiB %sMHz, health %s, %s'C, %s %s" % (
            "  "*level,
            memory.identity,
            memory.name,
            memory.status.state,
            memory.capacity_mib,
            memory.operating_speed_mhz,
            ",".join(memory.metrics.health),
            memory.metrics.temperature_celsius,
            memory.memory_device_type,
            memory.manufacturer))
    if not level:
        print()


### STORAGE ###

def get_storages(rsd):
    return rsd.get_storage_service_collection().get_members()

def get_storage(rsd, path):
    assert "StorageServices" in path
    assert path.count("/") >= 4
    ss_path = "/".join(path.split("/", 5)[:5])
    ss = rsd.get_storage_service(ss_path)
    return ss

def show_storage(rsd, ss):
    assert isinstance(ss,
            rsd_lib.resources.v2_3.storage_service.StorageService)
    utils.print_header("Storage", ss)
    print("hosting system:")
    system = ss.json["Links"]["HostingSystem"]["@odata.id"]
    system = get_system(rsd, system)
    show_systems(rsd, [system], 1)
    print("drives:")
    show_drives(rsd, ss.drives.get_members(), 1)
    print("pools:")
    show_pools(rsd, ss.storage_pools.get_members(), 1)
    print("volumes:")
    show_volumes(rsd, ss.volumes.get_members(), 1)
    print("managers:")
    for m in ss.json["Links"]["Oem"]["Intel_RackScale"]["ManagedBy"]:
        print("  %s" % m["@odata.id"])
    print()

def show_storages(rsd, sses, level=0):
    for ss in sses:
        assert isinstance(ss,
                rsd_lib.resources.v2_3.storage_service.StorageService)
        print("%s%s \"%s\": %s, system %s, drives %s, "
              "pools %s, volumes %s, mgrs %s" % (
            "  "*level,
            ss.identity,
            ss.name,
            ss.status.state,
            ss.json["Links"]["HostingSystem"]["@odata.id"].split("/")[-1],
            len(ss.drives.members_identities),
            len(ss.storage_pools.members_identities),
            len(ss.volumes.members_identities),
            len(ss.json["Links"]["Oem"]["Intel_RackScale"]["ManagedBy"])))
    print()


### VOLUME COLLECTION ###

def get_volume_collections(rsd, filter_nvme=True):
    storage_services = get_storages(rsd)
    ret = []
    for storage in storage_services:
        if filter_nvme:
            drives = storage.drives.get_members()
            if drives and \
                    any(map(lambda ident: 'nvme' in ident.durable_name,
                            drives[0].identifiers)):
                ret.append(storage.volumes)
        else:
            ret.append(storage.volumes)
    return ret

def _safe_int(i):
    try:
        return int(i)
    except Exception:
        return 0

def _safe_bytes_to_gigs(i):
    return _safe_int(i)/1024.0/1024/1024


### STORAGE/POOL ###

def get_pools(rsd):
    storages = get_storages(rsd)
    pools = []
    for storage in storages:
        pools.extend(storage.storage_pools.get_members())
    return pools

def get_pool(rsd, path):
    ss = get_storage(rsd, path)
    assert("StoragePools" in path)
    assert path.count("/") == 6
    pool = ss.storage_pools.get_member(path)
    return pool

def show_pool(rsd, pool):
    assert(isinstance(pool,
        rsd_lib.resources.v2_3.storage_service.storage_pool.StoragePool))
    utils.print_header("Pool", pool)
    print("block size: %s" % pool.json["BlockSizeBytes"])
    print("capacity:")
    capacity = pool.capacity
    print("  allocated: %sGiB" % _safe_bytes_to_gigs(capacity.allocated_bytes))
    print("  consumed: %sGiB" % _safe_bytes_to_gigs(capacity.consumed_bytes))
    print("  provisioned: %sGiB" % _safe_bytes_to_gigs(capacity.provisioned_bytes))
    print("  guaranteed: %sGiB" % _safe_bytes_to_gigs(capacity.guaranteed_bytes))
    print("capacity sources:")
    cses = pool.capacity_sources
    for cs in cses:
        print("  > a/c/p/g: %s/%s/%s/%s GiB from drives" % (
            _safe_bytes_to_gigs(cs.provided_capacity.allocated_bytes),
            _safe_bytes_to_gigs(cs.provided_capacity.consumed_bytes),
            _safe_bytes_to_gigs(cs.provided_capacity.provisioned_bytes),
            _safe_bytes_to_gigs(cs.provided_capacity.guaranteed_bytes)))
        drives = cs.providing_drives
        drives = [get_drive(rsd, dv) for dv in drives]
        show_drives(rsd, drives, 2)
    print("allocated volumes:")
    show_volumes(rsd, pool.allocated_volumes.get_members(), 1)
    print("allocated pools: %s" % len(pool.allocated_pools.get_members()))
    print("identifiers: %s" % pool.identifiers)
    print()

def show_pools(rsd, pools, level=0):
    for pool in pools:
        assert(isinstance(pool,
            rsd_lib.resources.v2_3.storage_service.storage_pool.StoragePool))
        print("%s%s \"%s\": %s %s(%s)GiB %s, s_drives: %s, pools: %s, vols: %s" % (
            "  "*level,
            pool.identity,
            pool.name,
            pool.status.state,
            _safe_bytes_to_gigs(pool.capacity.allocated_bytes),
            _safe_bytes_to_gigs(pool.capacity.consumed_bytes),
            pool.json["BlockSizeBytes"],
            len(pool.capacity_sources),
            len(pool.allocated_pools.get_members()),
            len(pool.allocated_volumes.get_members())
            ))
    if not level:
        print()


### STORAGE/VOLUME ###

def get_volumes(rsd):
    storages = get_storages(rsd)
    volumes = []
    for storage in storages:
        volumes.extend(storage.volumes.get_members())
    return volumes

def get_volume(rsd, path):
    ss = get_storage(rsd, path)
    assert "Volumes" in path
    assert path.count("/") == 6
    vol = ss.volumes.get_member(path)
    return vol

def show_volume(rsd, volume):
    assert isinstance(volume,
            rsd_lib.resources.v2_3.storage_service.volume.Volume)

    utils.print_header("Volume", volume)
    print("capacity: %s Gib" % _safe_bytes_to_gigs(volume.capacity_bytes))
    print("allocated: %s Gib" % _safe_bytes_to_gigs(volume.allocated_Bytes))
    print("block size: %s" % volume.json["BlockSizeBytes"])
    utils.print_body(volume, (
        "bootable",
        "erased",
        "erase_on_detach"))
    print("identifiers:")
    for ident in volume.identifiers:
        print("  %s: %s" % (ident.durable_name_format,
            ident.durable_name))
    print("capability sources:")
    for cap in volume.capacity_sources:
        print("  > %s GiB from pools:" %
              _safe_bytes_to_gigs(cap.allocated_Bytes))
        pools = cap.providing_pools
        pools = [get_pool(rsd, pool) for pool in pools]
        show_pools(rsd, pools, 2)
    print("links:")
    assert(2 == len(volume.links.keys()))
    print("  endpoints:")
    endpoints = volume.links.endpoints
    endpoints = [get_endpoint(rsd, ep) for ep in endpoints]
    show_endpoints(rsd, endpoints, 2)
    print("  drives:")
    for d in volume.json["Links"]["Drives"]:
        print("    !! %s" % d)
    if volume.links.metrics:
        print("  metrics:")
        for m in volume.links.metrics:
            print("    !! %s" % m)
    else:
        print("  metrics: None")
    utils.print_body_none(volume, (
        "manufacturer",
        "model",
        "replica_infos",
        "access_capabilities"))
    print()

def show_volumes(rsd, volumes, level=0):
    for vol in volumes:
        assert isinstance(vol,
                rsd_lib.resources.v2_3.storage_service.volume.Volume)
        dev_path = ""
        if vol.identifiers:
            for ident in vol.identifiers:
                if ident.durable_name_format == "SystemPath":
                    dev_path = " %s" % ident.durable_name
                    break
        print("%s%s \"%s\": %s%s %s(%s)GiB %s, s_pools %s, endp %s, drives %s" % (
            "  "*level,
            vol.identity,
            vol.name,
            vol.status.state,
            dev_path,
            _safe_bytes_to_gigs(vol.capacity_bytes),
            _safe_bytes_to_gigs(vol.allocated_Bytes),
            vol.json["BlockSizeBytes"],
            len(vol.capacity_sources),
            len(vol.links.endpoints),
            len(vol.json["Links"]["Drives"])))
    if not level:
        print()

def _try_create_volume(rsd, vol_col, size_in_gb):
    try:
        volume_size = size_in_gb * GIGABYTE
        vol_url = vol_col.create_volume(volume_size)
        print("created volume: %s" % vol_url)
        vol_col.refresh()
        volume = vol_col.get_member(vol_url)
        if volume:
            return volume
        else:
            raise RuntimeError("Cannot find created volume: %s" % vol_url)
    except (sushy_exceptions.HTTPError, sushy_exceptions.ConnectionError) as e:
        return None

def create_volume(rsd, size_in_gb):
    volume = None
    for vc in get_volume_collections(rsd):
        volume = _try_create_volume(rsd, vc, size_in_gb)
        if volume:
            break
    if not volume:
        print("Cannot create volume in %dG" % size_in_gb)
    return volume

def clone_volume(rsd, volume):
    ret = None


def get_volume_stats(rsd):
    def _get_storages(rsd, filter_nvme=True):
        ret = []
        for storage in rsd.get_storage_service_collection().get_members():
            if filter_nvme:
                drives = storage.drives.get_members()
                if drives and \
                        any(map(lambda ident: 'nvme' in ident.durable_name,
                                drives[0].identifiers)):
                    ret.append(storage)
            else:
                ret.append(storage)
        return ret

    free_capacity_gb = 0
    total_capacity_gb = 0
    allocated_capacity_gb = 0
    total_volumes = 0
    ret = []
    try:
        storages = _get_storages(rsd)
        for storage in storages:
            for pool in storage.storage_pools.get_members():
                total_capacity_gb += \
                        float(pool.capacity.allocated_bytes or 0)/GIGABYTE
                allocated_capacity_gb += \
                        float(pool.capacity.consumed_bytes or 0)/GIGABYTE
            total_volumes += len(storage.volumes.members_identities)
    except Exception as e:
        print(e)
        return None

    free_capacity_gb = total_capacity_gb - allocated_capacity_gb
    print("vol stats: free %sG, allocated %sG, total %sG, %s volumes" % (
          free_capacity_gb,
          allocated_capacity_gb,
          total_capacity_gb,
          total_volumes))


### CHASSIS ###

def get_chassises(rsd):
    return rsd.get_chassis_collection().get_members()

def get_chassis(rsd, path):
    assert("Chassis" in path)
    assert(path.count("/") >= 4)
    chs_path = "/".join(path.split("/", 5)[:5])
    chassis = rsd.get_chassis(chs_path)
    return chassis

def show_chassis(rsd, chassis):
    assert isinstance(chassis, rsd_lib.resources.v2_1.chassis.Chassis)
    utils.print_header("Chassis", chassis)
    utils.print_body(chassis, (
        "chassis_type",
        "oem"))

    print("links:")
    links = chassis.json["Links"]
    assert(11 == len(links))

    print("  systems:")
    systems = (get_system(rsd, val["@odata.id"]) for val in
    links["ComputerSystems"])
    show_systems(rsd, systems, 2)

    print("  ContainedBy:")
    contained = links["ContainedBy"]
    if contained:
        contained = contained.get("@odata.id")
        contained = get_chassis(rsd, contained)
        show_chassises(rsd, [contained], 2)

    print("  Contains:")
    contains = (get_chassis(rsd, c["@odata.id"]) for c in links["Contains"])
    show_chassises(rsd, contains, 2)

    print("  CooledBy:")
    for c in links["CooledBy"]:
        print("    %s" % c)

    print("  Drives:")
    drives = [val["@odata.id"] for val in links["Drives"]]
    drives = [get_drive(rsd, drv) for drv in drives]
    show_drives(rsd, drives, 2)

    print("  ManagedBy:")
    managers = (val["@odata.id"] for val in links["ManagedBy"])
    for m in managers:
        #TODO
        print("    %s" % m)

    print("  ManagersInChassis:")
    managers = (val["@odata.id"] for val in
            links["ManagersInChassis"])
    for m in managers:
        print("    %s" % m)

    print("  Oem switches:")
    for s in links["Oem"]["Intel_RackScale"]["Switches"]:
        print("    %s" % s)

    print("  PoweredBy:")
    for p in links["PoweredBy"]:
        print("    %s" % p)

    print("  Storage:")
    storages = (val["@odata.id"] for val in links["Storage"])
    for s in storages:
        #TODO /redfish/v1/Systems/3-s-2/Storage/3-s-2-sr-1
        print("    %s" % s)

    utils.print_body_none(chassis, (
        "asset_tag",
        "part_number",
        "serial_number",
        "sku"))
    print()


def show_chassises(rsd, chassises, level=0):
    for chassis in chassises:
        assert isinstance(chassis, rsd_lib.resources.v2_1.chassis.Chassis)
        links = chassis.json["Links"]
        parent = "Non"
        contained = links["ContainedBy"]
        if contained:
            parent = contained["@odata.id"].split("/")[-1]

        print("%s%s \"%s\": %s %s, parent %s, childs %s, "
              "sys %s, coolby %s, drives %s, "
              "mgrs %s/%s, switches %s, powers %s, stos %s" % (
            "  "*level,
            chassis.identity,
            chassis.name,
            chassis.status.state,
            chassis.chassis_type,
            parent,
            len(links["Contains"]),
            len(links["ComputerSystems"]),
            len(links["CooledBy"]),
            len(links["Drives"]),
            len(links["ManagedBy"]),
            len(links["ManagersInChassis"]),
            len(links["Oem"]["Intel_RackScale"]["Switches"]),
            len(links["PoweredBy"]),
            len(links["Storage"])))
    if not level:
        print()


### CHASSIS/DRIVE ###

def get_drives(rsd):
    chassises = get_chassises(rsd)
    drives = []
    for chassis in chassises:
        links = chassis.json["Links"]
        drives_ = [val["@odata.id"] for val in links["Drives"]]
        drives_ = [get_drive(rsd, drv) for drv in drives_]
        drives.extend(drives_)
    return drives

def get_drive(rsd, path, ss=None):
    if ss:
        assert isinstance(ss,
                rsd_lib.resources.v2_3.storage_service.StorageService)
    else:
        ss = get_storages(rsd)[0]
    assert("Drives" in path)
    assert(path.count("/") == 6)
    drive = ss.drives.get_member(path)
    return drive

def show_drive(rsd, drive):
    assert isinstance(drive,
            rsd_lib.resources.v2_3.storage_service.drive.Drive)
    utils.print_header("Drive", drive)
    print("capacity: %s GiB" % _safe_bytes_to_gigs(drive.capacity_bytes))
    print("block size: %s" % drive.json["BlockSizeBytes"])
    utils.print_body(drive, (
        "manufacturer",
        "media_type",
        "model",
        "protocol",
        "serial_number"))
    utils.print_items(drive, "oem")
    print("identifiers:")
    for ident in drive.identifiers:
        print("  %s: %s" % (
            ident.durable_name_format,
            ident.durable_name))
    print("used by:")
    pools = [v["@odata.id"] for v in
             drive.json["Oem"]["Intel_RackScale"]["UsedBy"]]
    pools = [get_pool(rsd, url) for url in pools]
    show_pools(rsd, pools, 1)
    storage = drive.oem.storage
    if storage:
        print("storage: %s" % storage["@odata.id"])
    else:
        print("storage: None")
    print("links:")
    links = drive.links
    assert(3 == len(links.keys()))
    print("  chassis:")
    chassis = drive.links.chassis
    chassis = get_chassis(rsd, chassis)
    show_chassises(rsd, [chassis], 2)
    print("  endpoints:")
    endpoints = [get_endpoint(rsd, ep) for ep in drive.links.endpoints]
    show_endpoints(rsd, endpoints, 2)
    print("  volumes:")
    volumes = drive.links.volumes
    volumes = [get_volume(rsd, url) for url in volumes]
    show_volumes(rsd, volumes, 2)
    utils.print_body_none(drive, (
        "asset_tag", "capable_speed_gbs", "drive_type", "indicator_led",
        "location", "negotiated_speed_gbs", "part_number",
        "predicted_media_life_left_percent", "revision", "rotation_speed_rpm",
        "sku", "status_indicator"))
    print()

def show_drives(rsd, drives, level=0):
    for drive in drives:
        assert isinstance(drive,
                rsd_lib.resources.v2_3.storage_service.drive.Drive)
        device_dir = ""
        if drive.identifiers:
            device_dir = " " + drive.identifiers[0].durable_name
        sto = "N/A"
        if drive.oem.storage:
            sto = drive.oem.storage["@odata.id"].split("/")[-1]
        print("%s%s \"%s\": %s %.4f GiB, %s %s %s%s, "
                "chassis %s, sto %s, endp %s, vols %s, pools %s" % (
            "  "*level,
            drive.identity,
            drive.name,
            drive.status.state,
            _safe_bytes_to_gigs(drive.capacity_bytes),
            drive.json["BlockSizeBytes"],
            drive.media_type,
            drive.protocol,
            device_dir,
            drive.links.chassis.split("/", -1)[-1],
            sto,
            len(drive.links.endpoints),
            len(drive.links.volumes),
            len(drive.json["Oem"]["Intel_RackScale"]["UsedBy"])))
    if not level:
        print()


### FABRIC ###

def get_fabrics(rsd):
    return rsd.get_fabric_collection().get_members()

def get_fabric(rsd, path):
    assert "Fabrics" in path
    assert path.count("/") >= 4
    fab_path = "/".join(path.split("/", 5)[:5])
    fabric = rsd.get_fabric(fab_path)
    return fabric

def show_fabric(rsd, fabric):
    assert isinstance(fabric, rsd_lib.resources.v2_3.fabric.Fabric)
    utils.print_header("Fabric", fabric)
    utils.print_body(fabric, (
        "fabric_type",
        "max_zones"))
    print("zones:")
    show_zones(rsd, fabric.zones.get_members(), 1)
    print("endpoints:")
    show_endpoints(rsd, fabric.endpoints.get_members(), 1)
    print()

def show_fabrics(rsd, fabrics, level=0):
    for fabric in fabrics:
        assert isinstance(fabric, rsd_lib.resources.v2_3.fabric.Fabric)
        print("%s%s \"%s\": %s %s, max zones %s, zones %s, endpoints %s" % (
            "  "*level,
            fabric.identity,
            fabric.name,
            fabric.status.state,
            fabric.fabric_type,
            fabric.max_zones,
            len(fabric.zones.members_identities),
            len(fabric.endpoints.members_identities)))
    if not level:
        print()


### FABRIC/ZONE ###

def get_zones(rsd):
    fabrics = get_fabrics(rsd)
    zones = []
    for fabric in fabrics:
        zones.extend(fabric.zones.get_members())
    return zones

def get_zone(rsd, path):
    fabric = get_fabric(rsd, path)
    assert "Zones" in path
    assert path.count("/") == 6
    zone = fabric.zones.get_member(path)
    return zone

def show_zone(rsd, zone):
    assert isinstance(zone, rsd_lib.resources.v2_3.fabric.zone.Zone)
    utils.print_header("Zone", zone)
    print("endpoints:")
    # BUG
    endpoints = [get_endpoint(rsd, e.path) for e in zone.endpoints]
    show_endpoints(rsd, endpoints, 1)
    assert(1 == len(zone.links.keys()))
    assert(len(zone.endpoints) == len(zone.links.endpoint_identities))
    print()

def show_zones(rsd, zones, level=0):
    for zone in zones:
        assert isinstance(zone, rsd_lib.resources.v2_3.fabric.zone.Zone)
        print("%s%s \"%s\": %s %s" % (
            "  "*level,
            zone.identity,
            zone.name,
            zone.status.state,
            ",".join(i.split("/")[-1] for i in zone.links.endpoint_identities)))
    if not level:
        print()


### FABRIC/ENDPOINT ###

def get_endpoints(rsd):
    fabrics = get_fabrics(rsd)
    endpoints = []
    for fab in fabrics:
        endpoints.extend(fab.endpoints.get_members())
    return endpoints

def get_endpoint(rsd, path):
    fabric = get_fabric(rsd, path)
    assert "Endpoints" in path
    assert path.count("/") == 6
    endpoint = fabric.endpoints.get_member(path)
    return endpoint

def show_endpoint(rsd, endpoint):
    assert isinstance(endpoint,
            rsd_lib.resources.v2_3.fabric.endpoint.Endpoint)

    utils.print_header("Endpoint", endpoint)
    utils.print_body(endpoint, ("protocol",))
    assert(1 == len(endpoint.oem))
    utils.print_items(endpoint, "oem.authentication")

    print("connected entities:")
    for c_ent in endpoint.connected_entities:
        if c_ent.entity_role == "Initiator":
            print("  %s-%s:" % (c_ent.entity_role,
                                c_ent.entity_type))
            system = get_system(rsd, c_ent.entity_link)
            show_systems(rsd, [system], 2)
        elif c_ent.entity_role == "Target":
            print("  %s-%s:" % (c_ent.entity_role,
                                c_ent.entity_type))
            volume = get_volume(rsd, c_ent.entity_link)
            show_volumes(rsd, [volume], 2)
        else:
            print("  %s-%s: %s" % (c_ent.entity_role,
                                   c_ent.entity_type,
                                   c_ent.entity_link))
    print("identifiers:")
    for ident in endpoint.identifiers:
        print("  %s: %s" % (ident.name_format, ident.name))
    print("ip transport:")
    for ip in endpoint.ip_transport_details:
        print("  %s://%s(%s):%s" % (ip.transport_protocol,
                                   ip.ipv4_address,
                                   ip.ipv6_address,
                                   ip.port))
    print("links:")
    links = endpoint.links
    assert(4 == len(links))
    print("  zones:")
    zones = links.zones
    zones = [get_zone(rsd, url) for url in zones]
    show_zones(rsd, zones, 2)
    print("  endpoints: %s" % links.endpoints)
    print("  interface: %s" % links.interface)
    print("  ports: ")
    for port in links.ports:
        print("    %s" % port)
    print()

def show_endpoints(rsd, endpoints, level=0):
    for endpoint in endpoints:
        assert isinstance(endpoint,
                rsd_lib.resources.v2_3.fabric.endpoint.Endpoint)
        links_str = "; zones %s, ports %s" % (
                len(endpoint.links.zones),
                len(endpoint.links.ports))
        ident_str = ",".join("%s=%s" %
                (ident.name_format,
                 ident.name)
                for ident in endpoint.identifiers)
        if ident_str:
            ident_str = "; " + ident_str
        conn_str = ",".join("%s=%s" %
                (ent.entity_role,
                 ent.entity_link.split("/")[-1])
                for ent in endpoint.connected_entities)
        if conn_str:
            conn_str = "; " + conn_str
        ip_str = ",".join("%s://%s(%s):%s" %
                (ip.transport_protocol,
                 ip.ipv4_address,
                 ip.ipv6_address,
                 ip.port)
                for ip in endpoint.ip_transport_details)
        if ip_str:
            ip_str = "; " + ip_str

        print("%s%s \"%s\": %s %s%s%s%s%s" % (
            "  "*level,
            endpoint.identity,
            endpoint.name,
            endpoint.status.state,
            endpoint.protocol,
            links_str,
            ident_str,
            conn_str,
            ip_str))
    if not level:
        print()


#####################

def _get_endpoint_nqn(rsd, endpoint_uri):
    j_endp = json.loads(rsd._conn.get(endpoint_uri).text)
    nqn = None
    for ident in j_endp["Identifiers"]:
        if ident["DurableNameFormat"] == "NQN":
            nqn = ident["DurableName"]
            break
    if not nqn:
        return None
    else:
        nqn = nqn.split("nqn.2014-08.org.nvmexpress:uuid:")[-1]
        return nqn

def get_volume_endpoint_nqns(rsd, volume):
    try:
        nqns = []
        for endp_url in volume.links.endpoints:
            nqn = _get_endpoint_nqn(rsd, endp_url)
            if nqn:
                nqns.append(nqn)
        return nqns
    except Exception as e:
        raise RuntimeError("Cannot find NQN for volume %s: %s"
                % (volume.path, e))

def get_node_endpoint_nqns(rsd, node):
    try:
        nqns = []
        endpoints = node.system.json["Links"]["Endpoints"]
        for endpoint in endpoints:
            endp_url = endpoint["@odata.id"]
            nqn = _get_endpoint_nqn(rsd, endp_url)
            if nqn:
                nqns.append(nqn)
        return nqns
    except Exception as e:
        raise RuntimeError("Cannot find NQN for node %s: %s"
                % (node.path, e))

NQN_PREFIX = 'nqn.2014-08.org.nvmexpress:uuid:'
def attach_volume(rsd, node, volume):
    def _get_nqn_endpoints(rsd, endpoint_urls):
        ret = []
        for endpoint_url in endpoint_urls:
            endpoint_json = \
                    json.loads(rsd._conn.get(endpoint_url).text)
            for ident in endpoint_json["Identifiers"]:
                if ident["DurableNameFormat"] == "NQN":
                    nqn = ident["DurableName"]
                    nqn = nqn.split(NQN_PREFIX)[-1]
                    ret.append((nqn, endpoint_json))
                    break
        return ret

    if volume.path not in node.get_allowed_attach_endpoints():
        raise RuntimeError("Not allowed to attach volume")
    num_vol_endpoints = len(volume.links.endpoints)
    if (num_vol_endpoints != 0):
        raise RuntimeError("Volume already attached")
    node.refresh()
    node.attach_endpoint(volume.path)
    print("attached volume to node: %s -> %s" %
            (volume.identity, node.identity))
    volume.refresh()
    node.refresh()
    num_vol_endpoints_after = len(volume.links.endpoints)
    if num_vol_endpoints+1 != num_vol_endpoints_after:
        raise RuntimeError("Attach volume error: endpoints %d -> %d"
                % (num_vol_endpoints, num_vol_endpoints_after))
    target_nqns = get_volume_endpoint_nqns(rsd, volume)

    v_endpoints = volume.links.endpoints
    v_endpoints = _get_nqn_endpoints(rsd, v_endpoints)
    if len(v_endpoints) != 1:
	return "Attach volume error: %d target nqns" % len(v_endpoints)
    target_nqn, v_endpoint = v_endpoints[0]
    ip_transports = v_endpoint["IPTransportDetails"]
    if len(ip_transports) != 1:
	return "Attach volume error: %d target ips" % len(ip_transports)
    ip_transport = ip_transports[0]
    target_ip = ip_transport["IPv4Address"]["Address"]
    target_port = ip_transport["Port"]

    if len(target_nqns) != 1:
        raise RuntimeError("Attach volume error: %d target nqns"
                % len(target_nqns))
    initiator_nqns = get_node_endpoint_nqns(rsd, node)
    if len(initiator_nqns) == 0:
        raise RuntimeError("Attach volume error: %d initiator nqns"
                % len(initiator_nqns))
    print("attached nqn info: %s, %s" %
            (target_nqns[0], initiator_nqns[0]))
    assert(target_nqn == target_nqns[0])
    return target_nqns[0], initiator_nqns[0], target_ip, target_port

def detach_volume(node, volume):
    if volume.path not in node.get_allowed_detach_endpoints():
        raise RuntimeError("Not allowed to detach volume")
    num_vol_endpoints = len(volume.links.endpoints)
    node.refresh()
    node.detach_endpoint(volume.path)
    print("detached volume from node: %s -//-> %s" %
            (volume.identity, node.identity))
    volume.refresh()
    node.refresh()
    num_vol_endpoints_after = len(volume.links.endpoints)
    if num_vol_endpoints-1 != num_vol_endpoints_after:
        raise RuntimeError("Detach volume error: endpoints %d -> %d"
                % (num_vol_endpoints, num_vol_endpoints_after))
