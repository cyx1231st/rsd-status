#!/opt/miniconda/bin/python
from __future__ import print_function

from pprint import pprint
from utils import *

def run():
    rsd = init()

#### resources ####
    # print("fabric...")
    # fabrics = get_fabrics(rsd)
    # fabric = fabrics[0]
    # print("  zone...")
    # zones = get_zones(rsd)
    # zone = zones[0]
    # print("  endpoint...")
    # endpoints = get_endpoints(rsd)
    # endpoint = endpoints[0]

    # print("node...")
    # nodes = get_nodes(rsd)
    # node = nodes[0]

    # print("system...")
    # systems = get_systems(rsd)
    # system = systems[6]
    # print("  interface...")
    # interfaces = get_interfaces(rsd)
    # interface = interfaces[0]
    # print("  memory...")
    # memories = get_memories(rsd)
    # memory = memories[0]
    # print("  processor...")
    # processors = get_processors(rsd)
    # processor = processors[0]

    # print("storage...")
    # storages = get_storages(rsd)
    # storage = storages[0]
    # print("  pool...")
    # pools = get_pools(rsd)
    # pool = pools[0]
    # print("  volume...")
    # volumes = get_volumes(rsd)
    # volume = volumes[4]

    # print("chassis...")
    # chassises = get_chassises(rsd)
    # chassis = chassises[0]
    # print("  drive...")
    # drives = get_drives(rsd)
    # drive = drives[0]

    # print("switch...")
    # switches = rsd.get_ethernet_switch_collection()
    # # switch = switches.get_members()[0]

    # print("manager...")
    # managers = rsd.get_manager_collection()
    # manager = managers.get_members()[0]
    print("Done!")
    print()

#### showing ####
    import pdb; pdb.set_trace()

    # fab_ = get_fabric(rsd, fabric.path)
    # show_fabric(rsd, fab_)
    # show_fabrics(rsd, fabrics)
    # for fabric in fabrics:
    #     show_fabric(rsd, fabric)
    # zone_ = get_zone(rsd, zone.path)
    # show_zone(rsd, zone_)
    # show_zones(rsd, zones)
    # for zone in zones:
    #     show_zone(rsd, zone)
    # endpoint_ = get_endpoint(rsd, endpoint.path)
    # show_endpoint(rsd, endpoint_)
    # show_endpoints(rsd, endpoints)
    # for endpoint in endpoints:
    #     show_endpoint(rsd, endpoint)

    # node_ = get_node(rsd, node.path)
    # show_node(rsd, node_)
    # show_nodes(rsd, nodes)
    # for node in nodes:
    #     show_node(rsd, node)

    # system_ = get_system(rsd, system.path)
    # show_system(rsd, system_)
    # show_systems(rsd, systems)
    # for system in systems:
    #     show_system(rsd, system)
    # interface_ = get_interface(rsd, interface.path)
    # show_interface(rsd, interface_)
    # show_interfaces(rsd, interfaces)
    # for interface in interfaces:
    #     show_interface(rsd, interface)
    # memory_ = get_memory(rsd, memory.path)
    # show_memory(rsd, memory_)
    # show_memories(rsd, memories)
    # for memory in memories:
    #     show_memory(rsd, memory)
    # processor_ = get_processor(rsd, processor.path)
    # show_processor(rsd, processor_)
    # show_processors(rsd, processors)
    # for processor in processors:
    #     show_processor(rsd, processor)

    # storage_ = get_storage(rsd, storage.path)
    # show_storage(rsd, storage_)
    # show_storages(rsd, storages)
    # for storage in storages:
    #     show_storage(rsd, storage)
    # pool_ = get_pool(rsd, pool.path)
    # show_pool(rsd, pool_)
    # show_pools(rsd, pools)
    # for pool in pools:
    #     show_pool(rsd, pool)
    # volume_ = get_volume(rsd, volume.path)
    # show_volume(rsd, volume_)
    # show_volumes(rsd, volumes)
    # for volume in volumes:
    #     show_volume(rsd, volume)

    # chassis_ = get_chassis(rsd, chassis.path)
    # show_chassis(rsd, chassis_)
    # show_chassises(rsd, chassises)
    # for chassis in chassises:
    #     show_chassis(rsd, chassis)
    # drive_ = get_drive(rsd, drive.path)
    # show_drive(rsd, drive_)
    # show_drives(rsd, drives)
    # for drive in drives:
    #     show_drive(rsd, drive)

    import pdb; pdb.set_trace()


if __name__ == "__main__":
    run()
