#!/usr/bin/python
import httplib
import ConfigParser
import json
import os, sys
from pprint import pprint
import subprocess
import socket
import logging
import time
import re
import base64

sys.path.insert(0, "/opt/milio/")
from libs.atlas.cmd import runcmd
from libs.atlas.atl_storage import scsi_to_device, lvm_free_space

sys.path.insert(0, "/opt/milio/libs/atlas/")
from log import *
LOG_FILENAME = '/var/log/usx-agstart.log'

sys.path.insert(0, "/opt/milio/atlas/roles/utils/")
from ibdserver_conf import config_support_server, reset_ibdserver_config



SEC_GENERIC = "global"
PORT = "port"
LISTENADDR = "ip"
NUM_WORKERS = "num_workers"
EXPORTNAME = "exportname"
DEV = "dev"
FILESIZE = "filesize"
SIZE = "size"
MEM_DEVICE = "/dev/ram0"
MEM_DIR = "/mnt/memory/"
DISK_DIR = "/mnt/disk/"
IBD_CONFIG_FILE = "/etc/ilio/ibdserver.conf"
IBD_DEF_NUM_WORKERS = 4

# raidplanner and agstart MUST IN SYNC.
PRIMARY_DISK_PREFIX = "PRIMARY_DISK_"
PRIMARY_FLASH_PREFIX = "PRIMARY_FLASH_"

# commands
CMD_ATLASROLE_DIR = "/opt/milio/atlas/roles"
CMD_AGGCREATE = CMD_ATLASROLE_DIR + "/aggregate/agexport.pyc" + " -c "

######## Code to get First available assigned IP address if required.
# Code is from: http://stackoverflow.com/questions/11735821/python-get-localhost-ip
if os.name != "nt":
    import fcntl
    import struct


def get_interface_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s',
                                                                        ifname[:15]))[20:24])


def get_lan_ip():
    try:
        ip = socket.gethostbyname(socket.gethostname())
    except:
        debug('WARNING : Exception getting IP from gethostbyname')
        ip = None
    debug('IP from gethostbyname: %s' % ip)
    if ip is None or (ip.startswith("127.") and os.name != "nt"):
        interfaces = [
            "eth0",
            "eth1",
        ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    debug('IP autofound = %s ' % ip)
    return ip


######## END : Code to get first available IP

###### Utility funcs : TODO : Move them into a library
def total_sectors(dev):
    return int(os.popen('blockdev --getsz %s' % dev).read().strip())


def get_sector_size(dev):
    return int(os.popen('blockdev --getss %s' % dev).read().strip())


def bytes_to_sectors(sector_size, bytes):
    return bytes / sector_size


def clear_out_single_partition(disk, number):
    cmd = 'parted -s %s rm %s' % (disk, number)
    ret, msg = runcmd(cmd, print_ret=True)
    if (ret == 0):
        return True
    elif 'doesn\'t exist' in msg:
        return True
    else:
        return False


def clear_out_partitions(disk):
    ret = clear_out_single_partition(disk, '1')
    if (ret == False):
        return False
    ret = clear_out_single_partition(disk, '2')
    if (ret == False):
        return False
    return True


def check_partition_table(disk):
    cmd = 'parted -s %s print' % disk
    ret, msg = runcmd(cmd, print_ret=True)
    if (ret == 0):
        return (True, 'Success')
    if 'unrecognised disk label' in msg:
        cmd = 'parted -s %s mklabel msdos' % disk
        ret, msg = runcmd(cmd, print_ret=True)
        if (ret == 0):
            return (True, 'Success')
        return (False, msg)
    else:
        return (False, msg)


# 08/26/2011 - Vinodh
# We no longer use sfdisk but parted, this fixes the alignment problem
def partition(disk):
    debug('Partitioning disk %s, checking paritions...' % disk)
    ret, msg = check_partition_table(disk)
    if (ret == False):
        debug('Unable to initialize partition table on %s,\nerror is %s' % disk, msg)
        return False
    debug('Partitioning disk %s, clearing any existing paritions...' % disk)
    clear_out_partitions(disk)
    cmd = 'parted -s %s unit s mkpart primary ext3 %s 100%%' % (disk, '2048')
    debug('Partitioning disk %s, Creating partition, cmd=%s' % (disk, cmd))
    ret, msg = runcmd(cmd, print_ret=True)
    if (ret == 0):
        debug('Partitioning disk %s, Creating partition Succeeded' % disk)
        return True
    else:
        clear_out_partitions(disk)
        debug("ERROR : Failed to create partitions on %s" % disk)
        return False


######## END : Utility functions #############

### Get the size of the brd module if loaded
# returns -1 on error
def get_loaded_brd_size_in_kb():
    rdsize = -1
    try:
        rdsize_sectors = long(subprocess.check_output(["cat", "/sys/block/ram0/size"]).strip())
        rdsize = long((rdsize_sectors * 512) / 1024)
    except:
        rdsize = long(-1)

    return rdsize


### Set up the memory module with the correct size
def set_up_memory(memsize, expuuid, mem_type='tmpfs'):
    if mem_type == 'brd':
        if setup_memory_brd(memsize) != 0:
            return None
        return MEM_DEVICE
    elif mem_type == 'tmpfs':
        return setup_memory_tmpfs(memsize, expuuid)
    else:
        return setup_memory_zram(memsize)


### Set up the ramdisk use tmpfs
def setup_memory_tmpfs(memsize, expuuid):
    if memsize <= 0:
        debug('ERROR : Invalid size %s for setting up tmpfs. Needs to be >= 1 (units: GB)' % memsize)
        return None
    memsize_mb = long(long(memsize) * 1024)
    memfile = MEM_DIR + expuuid + '/' + 'bigfile'
    memdir = os.path.dirname(memfile)
    if os.system('mkdir -p %s' % memdir) != 0:
        debug('Can not create dir %s' % memdir)
        return None

    # FIXME: We might want to add some check here.
    debug('Forcefully umount anything at our mount point %s !' % (memdir))
    os.system('umount %s' % (memdir))

    debug('Creating tmpfs %s with size %sG.' % (memdir, str(memsize)))
    if os.system('mount -t tmpfs -o size=%sG none %s' % (str(memsize), memdir)) != 0:
        debug('Can not create tmpfs %s with size %sG.' % (memdir, str(memsize)))
        return None

    return memfile


### Set up the 'brd' ramdisk module
def setup_memory_brd(memsize):
    if memsize <= 0:
        debug('ERROR : Invalid size %s for setting up memory module. Needs to be >= 1 (units: GB)' % memsize)
        return 1
    memsize_kb = long(long(memsize) * 1024 * 1024)
    memsize_mb = long(long(memsize) * 1024)
    debug('Requested setting up uncompressed memory device with size = %s GB (%s KB), checking...' % (
        str(memsize), str(memsize_kb)))
    if os.system('lsmod | grep brd >> %s 2>&1' % LOG_FILENAME) == 0:
        # brd is loaded, check if it has been loaded with the correct size
        rdsize = get_loaded_brd_size_in_kb()
        if rdsize > 0:
            debug('uncompressed memory device already loaded with size = %s KB' % str(rdsize))
            if rdsize == memsize_kb:
                debug('uncompressed memory device already set up with the correct size, nothing to do :)')
                return 0

        # brd is loaded with the wrong size, attempt to unload it
        debug('Attempting to remove previously loaded uncompressed memory module...')
        if os.system('rmmod brd >> %s 2>&1' % LOG_FILENAME) != 0:
            debug(
                'ERROR : uncompressed Memory device module loaded previously, could not unload it for this run, exiting')
            sys.exit(96)

    # If we got here, we need to load brd
    debug('Setting up uncompressed memory device with size = %s GB (%s KB)' % (str(memsize), str(memsize_kb)))
    if os.system('modprobe brd rd_size=%s >> %s 2>&1' % (memsize_kb, LOG_FILENAME)) != 0:
        return 1
    debug('Pre-populating memory space for %s.' % MEM_DEVICE)
    return os.system(
        'dd if=/dev/zero of=%s oflag=direct bs=1M count=%s >> %s 2>&1' % (MEM_DEVICE, str(memsize_mb), LOG_FILENAME))


#
# Return filesystem free space in K bytes (1024).
#
def fs_free_space(mount_dir):
    msg = subprocess.check_output('df -k %s' % mount_dir, shell=True)
    debug(msg)
    line = msg.split('\n')[1]
    items = re.split('\s+', line)
    size = items[3]
    return long(size)


def setup_fs(devname, filename):
    mount_dir = os.path.dirname(filename)
    if os.system('mkfs -t ext4 -T largefile4 -m 1 ' + devname) != 0:
        debug('mkfs failed.')
        return 1
    if os.system('mkdir -p ' + mount_dir) != 0:
        debug('mkdir failed.')
        return 1
    if os.system('mount -t ext4 -o rw,noatime,data=writeback %s %s' % (devname, mount_dir)) != 0:
        debug('mount failed.')
        return 1
    size = fs_free_space(mount_dir)
    if os.system('echo "%s %s ext4 rw,noatime,data=writeback 0 0" >>/etc/fstab' % (devname, mount_dir)) != 0:
        debug('update /etc/fstab failed.')
        return 1

    """
	mkfs -t ext4 /dev/sdb1
	mkdir -p /mnt/disk
	mount /dev/sdb1 /mnt/disk
	#For example, your /dev/sdb1 is 50G, this is 50-1=49G (49*1024*1024*1024) in bytes.
	echo "/dev/sdb1 /mnt/disk ext4 rw,noatime 0 0" >>/etc/fstab
	"""
    return 0


def setup_lvm(devname_list, vgname):
    devnames = ' '.join(devname_list)

    cmd_str = '/sbin/pvremove -ff -y ' + devnames
    debug(cmd_str)
    os.system(cmd_str)

    cmd_str = '/sbin/pvcreate ' + devnames
    debug(cmd_str)
    if os.system(cmd_str) != 0:
        debug('pvcreate failed on %s' % str(devnames))
        return 1
    cmd_str = '/sbin/vgcreate %s %s' % (vgname, devnames)
    debug(cmd_str)
    if os.system(cmd_str) != 0:
        debug('vgcreate failed on %s:%s' % (str(vgname), str(devnames)))
        return 1
    return 0


def update_usxmanager_capacity(uuid, sizeG):
    cmd_str = "curl -k -X PUT http://127.0.0.1:8080/usxmanager/usx/inventory/servicevm/exports/" + uuid + '/' + str(
        sizeG)
    ret, msg = runcmd(cmd_str, print_ret=True)
    return 0


def config_storage(exports, storage_type, storage_prefix):
    primary_exp = None
    sub_storages = []

    for exp in exports:
        if exp['storageuuid'].startswith(storage_prefix) or \
                                        'virtual' in exp and exp['virtual'] and exp['type'].upper() == storage_type:
            primary_exp = exp

    if primary_exp == None:
        # AMC doesn't specify primary storage, pick the first one.
        for exp in exports:
            if exp['type'].upper() == storage_type:
                primary_exp = exp
                debug("INFO: AMC doesn't specify primary storage for %s, pick %s" % (
                    storage_type, primary_exp['storageuuid']))
                break

    if primary_exp == None:
        debug("INFO: Can not find primary storage for storage_type: %s!" % (storage_type))
        return 0

    debug("INFO: primary %s storage is %s" % (storage_type, str(primary_exp)))

    for exp in exports:
        if exp['type'].upper() == storage_type:
            if exp == primary_exp:
                debug("INFO: skip partition virtual primary device.")
                continue
            device_name = scsi_to_device(exp['scsibus'])
            rc = partition(device_name)
            if not rc:
                debug("ERROR: Failed to partition %s" % device_name)
                return rc
            sub_storages.append(device_name + '1')

            # Zero all sub devices free capacity, the primary device will have the total free capacity.
            if exp['storageuuid'] != primary_exp['storageuuid']:
                debug("INFO: Zeroing " + exp['storageuuid'])
                update_usxmanager_capacity(exp['uuid'], 0)
            else:
                debug("INFO: Skip zeroing " + exp['storageuuid'])

    if primary_exp != None:
        rc = setup_lvm(sub_storages, primary_exp['storageuuid'])
        if rc != 0:
            debug("ERROR: Failed to setup %s VG." % storage_type)
            return rc
        vol_size = lvm_free_space(primary_exp['storageuuid'])
        debug("INFO: Update capacity of %s:%s to %d" % (primary_exp['storageuuid'], primary_exp['uuid'], vol_size))
        update_usxmanager_capacity(primary_exp['uuid'], vol_size)

    return 0


def config_disk_flash(exports):
    rc = config_storage(exports, 'DISK', PRIMARY_DISK_PREFIX)
    if rc != 0:
        debug("ERROR: Failed to config DISK storage.")
        return rc

    rc = config_storage(exports, 'FLASH', PRIMARY_FLASH_PREFIX)
    if rc != 0:
        debug("ERROR: Failed to config FLASH storage.")
        return rc

    # Skip 'MEMORY' storage here.

    return 0


#
# Helpful message for the clueless
#
def usage():
    debug("Usage:" + sys.argv[0] + " config|start")
    debug("      config - Assumes first time configuration, and creates a partition")
    debug("               on any available disk/Flash block device on specified device list")
    debug("               after first removing any existing partitions on that device.")
    debug("               WARNING: This will erase any existing data on the device.")
    debug(" ")
    debug("      start - Assumes that this Aggregate node has already been configured")
    debug("              and simply makes the requested Memory/Disk/Flash devices")
    debug("              available to Pool Nodes.")


#
# Check if a given primary partition exists on a given device
# If no partition number is specified, check for partition 1
#
# Returns:
#		True : if partition exists
#		False : If partition does not exist or there was an error
# 
def check_for_primary_partition(device, partition="1"):
    if device is None or not device:
        debug("ERROR : No valid device specified for checking for existence of partition")
        return False

    try:
        debug("Checking whether device %s has partition %s" % (device, partition))
        cmd = "parted -s %s unit s print | grep primary | tr -s ' '" % device
        ret, msg = runcmd(cmd, print_ret=True)
        if ret == 0:
            data = [line.strip().split(' ') for line in msg.split('\n') if line.strip()]
            # debug('data is: %s and len=%s' % (data, str(len(data))))
            for line in data:
                if line[0] == partition:
                    debug("Found primary partition %s on device %s" % (partition, device))
                    return True

            # If we got here, we didn't find the partition
            debug("WARNING: Could not find partition %s on device %s" % (partition, device))
            return False

        else:
            debug('No primary partitions found on device %s' % device)
            return False
    except:
        debug("ERROR : Exception getting partition information for device %s, exception was: %s" % sys.exc_info[0])

    # IF we got here, it's an error
    return False


def disable_transparent_hugepage():
    if os.system('echo never > /sys/kernel/mm/transparent_hugepage/enabled') != 0:
        return 1
    if os.system('echo never > /sys/kernel/mm/transparent_hugepage/defrag') != 0:
        return 1
    return 0


"""
Modified September 11, 2014, For rebuild service vm exports in the event of hypervisor migration
"""

LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'
API_PREFIX = '/usxmanager/usx/inventory'


def load_conf_from_amc(apiurl, apistr):
    """
    Get configuration information from AMC
     Input: query string
     Return: response data
    """
    try:
        debug("load conf from amc: %s" % apistr)
        protocol = apiurl.split(':')[0]
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        apiaddr = apiurl.split('/')[2]  # like: 10.15.107.2:8443
        debug(apistr)
        debug(apiaddr)
        if use_https == True:
            conn = httplib.HTTPSConnection(apiaddr)
        else:
            conn = httplib.HTTPConnection(apiaddr)
        conn.request("GET", apistr)
        response = conn.getresponse()
        debug(response.status, response.reason)
        if response.status != 200 and response.reason != 'OK':
            return None
        else:
            data = response.read()
    except:
        debug("ERROR : Cannot connect to AMC to query")
        return None

    return data


def get_compound_device_export_config():
    """
    Based on export uuids from atlas.json, find volume resources associated with this export
      and get the compound device details from the volume resources
    """
    export_bricks = []

    if device_dict.has_key("export"):
        exp_uuid_list_str = ""
        svm_storageuuid_list = []

        vr_uuid_list = []
        vr_uuid_list_str = ""
        repair_bricks = []

        svm_exports = device_dict['export']
        for export in svm_exports:
            svm_storageuuid_list.append(export['storageuuid'])
            exp_uuid_list_str += export['uuid']
            exp_uuid_list_str += "%2C"  # construct API string with ","

        exp_uuid_list = exp_uuid_list_str[:-3]  # get rid of trailing comma
        apistr = API_PREFIX + '/servicevm/exports/batch?uuids=' + exp_uuid_list + '&fields=volumeresourceuuids'
        export_response_str = load_conf_from_amc(LOCAL_AGENT, apistr)
        if export_response_str == None:
            debug("ERROR : REST API call to get volume resource uuids failed!")
            sys.exit(110)
        export_response = json.loads(export_response_str)
        # debug(export_response['items'])
        for item in export_response['items']:
            if item:  # skip empty reponses (export has not been used by any volumes)
                for element in item['volumeresourceuuids']:
                    vr_uuid_list.append(element)

        vr_uuid_list = list(set(vr_uuid_list))  # get rid of duplicated volume resource uuids

        #  construct API strings, with HTML enconding of comma
        for vr_uuid in vr_uuid_list:
            vr_uuid_list_str += vr_uuid
            vr_uuid_list_str += "%2C"

            # debug(vr_uuid_list_str)
        raidplan_list = []
        vruuid_list = vr_uuid_list_str[:-3]  # get rid of trailing comma
        apistr = API_PREFIX + '/volume/resources/batch?uuids=' + vruuid_list + '&fields=raidplans'
        raidplan_response_str = load_conf_from_amc(LOCAL_AGENT, apistr)
        if raidplan_response_str == None:
            debug("ERROR : REST API call to get raidplans from volume resources failed!")
            sys.exit(110)
        raidplan_response = json.loads(raidplan_response_str)

        raidplans_list = raidplan_response['items']
        for item in raidplans_list:
            raidplans = item['raidplans']
            for plan in raidplans:
                plandetail = plan['plandetail']
                plandetail_dict = json.loads(plandetail)
                plandetail_str = json.dumps(plandetail_dict, indent=4, separators=(',', ': '))
                # debug(plandetail_str)
                subplans = plandetail_dict['subplans']
                for subplan in subplans:
                    raidbricks = subplan['raidbricks']
                    for raidbrick in raidbricks:
                        # debug(raidbrick)
                        # debug("========")
                        subdevices = raidbrick['subdevices']
                        for subdevice in subdevices:
                            if subdevice['storageuuid'] in svm_storageuuid_list:
                                raidbrick_str = json.dumps(raidbrick)
                                repair_bricks.append(raidbrick_str)

                                # debug(repair_bricks)
        export_bricks = list(set(repair_bricks))  # remove duplicate raidbrick entries
    # for brick in export_bricks:
    #             #brick_dict = json.loads(brick)
    #             #debug(brick_dict)
    #             debug(brick)

    return export_bricks


set_log_file(LOG_FILENAME)
debug("==== BEGIN AGG NODE CONFIG/START ====  ")
# Does this script need to be called in 'configure' mode?
# If True, then we need to partition any existing disk devices.
NEEDS_CONFIG = False
NEEDS_REBUILD = False

cmd_options = {
    "config",
    "start",
}

if len(sys.argv) < 2:
    usage()
    debug("ERROR: Incorrect number of arguments. Need at least one argument which is either 'config' or 'start'")
    exit(1)

cmd_type = sys.argv[1]

if cmd_type is None or not cmd_type:
    usage()
    debug("ERROR: Incorrect argument - %s. Argument has to be either 'config' or 'start'" % cmd_type)
    exit(1)

if cmd_type in cmd_options:
    if cmd_type.lower().strip().startswith('config'):
        NEEDS_CONFIG = True
        debug("Script has been called in 'config' mode, so assuming that we need to do first-time configuration!")
    else:
        debug("Script has been called in 'start' mode, so assuming that first-time configuration is already done!")
else:
    usage()
    debug("ERROR: Incorrect argument '%s'. Argument has to be either 'config' or 'start'" % cmd_type)
    exit(1)

# Open the Atlas JSON file and read the data into JSON object
try:
    myfile = open('/etc/ilio/atlas.json', 'r')
    data1 = myfile.read()
    debug('data1 is: %s' % data1)
except:
    debug('ERROR : Failed opening JSON file to read config data, cannot continue!')
    sys.exit(2)

if data1 is None or not data1:
    debug('ERROR : No data available in Atlas json file, exiting')
    sys.exit(91)

device_dict = json.loads(data1)
if not device_dict:
    debug('ERROR : No JSON data read from Atlas json file, exiting')
    sys.exit(92)

pprint(device_dict)
myfile.close

# Get the ILIO Role
try:
    ilio_role = device_dict["roles"][0]
except KeyError:
    debug('ERROR : ===NO ROLE FOUND=== : KeyError : roles not present in JSON')
    ilio_role = None

if not ilio_role:
    debug('ERROR : ILIO Role does not seem to be defined in Atlas JSON, exiting')
    sys.exit(93)

debug('ILIO ROLE = %s' % ilio_role)

# Get rebuilt flag
if device_dict.has_key("rebuilt"):
    NEEDS_REBUILD = device_dict["rebuilt"]

# If this ILIO's role is not an Aggregator node, exit with error
if not ilio_role.lower() == 'service_vm':
    debug('ERROR : ILIO Role defined in Atlas JSON does not seem to be Aggregator, exiting!')
    sys.exit(94)

if disable_transparent_hugepage() != 0:
    debug('WARNING: disable transparent_hugepage failed.')

# Get the IBD Listener IP address from JSON.
try:
    ilio_ibd_listener_ip = device_dict["nbd_listener_ip"]
except KeyError:
    debug('KeyError : nbd_listener_ip Not present in JSON')
    ilio_ibd_listener_ip = None

if not ilio_ibd_listener_ip:
    # No Listener IP defined in the JSON.
    # Get the first valid IP address we can find
    debug(
        'No valid device server listener IP specified in JSON, attempting to get the first valid IP Address assigned to this node...')
    try:
        ilio_ibd_listener_ip = get_lan_ip()
    except:
        debug('Exception getting ILIO IBD Listener IP!')
        ilio_ibd_listener_ip = None
    if not ilio_ibd_listener_ip:
        # Still didn't get a valid IP assigned to the system. Bail.
        debug('ERROR : Unable to determine a valid IP address for the device server listener, exiting!')
        sys.exit(95)

debug('IBD LISTENER IP = %s' % ilio_ibd_listener_ip)

# Set up the ConfigParser object into which we'll stick the ibd config
# Note that while we manipulate this config regardless of whether we've
# been called with the 'config' or 'start' parameter, we ONLY actually
# write this object to file if we've been called with the 'config' option.
cfg = ConfigParser.ConfigParser()

# setup "generic" section in the ibd config
cfg.add_section(SEC_GENERIC)
num_workers = int(device_dict["num_workers"])
if num_workers < 1:
    num_workers = IBD_DEF_NUM_WORKERS
debug('IBD num_workers set to : %d' % num_workers)
cfg.set(SEC_GENERIC, NUM_WORKERS, str(num_workers))
## NOV-07 : per Jin and Tony's recommendation, leave the 'generic' section
## empty and do not specify an IP or port on which to listen for client
## requests.
# cfg.set(SEC_GENERIC, LISTENADDR, ilio_ibd_listener_ip)


# PRocess the export section in the JSON
# Also, do the necessary config if we've been called in config mode
exports = device_dict["export"]
if not exports:
    debug('ERROR : No Exports section defined in JSON, exiting')
    sys.exit(97)

for exp in exports:
    exptype = None
    try:
        exptype = exp['type']
    except:
        debug('ERROR : No Export type defined in exports section in JSON, exiting')
        sys.exit(98)
    expsize = 0
    try:
        expsize = exp['size']
    except KeyError:
        if not exptype.lower().startswith('mem'):
            pass
        else:
            debug('ERROR : Export type MEMORY, but size is not defined in JSON, exiting')
            sys.exit(99)

    debug('    name/uuid...= %s' % exp['uuid'])
    debug('    name/storageuuid...= %s' % exp['storageuuid'])
    if exp.has_key('scsibus'):
        debug('    name/scsibus...= %s' % exp['scsibus'])
    debug('    type........= %s' % exptype)
    debug('    size (GB)...= %s' % expsize)
    debug(' ')

    # If our export type is memory, make sure that we set up the
    # memory device with the correct size
    if exptype.lower().startswith('mem'):
        # Add a section in our ibd config for this export
        # NOV-07 : per Tony and Jin's recommendation, we use the uuid as the
        # name for the export section. Changing this from 'type' to 'uuid'
        cfg.add_section(exp['storageuuid'])

        if expsize <= 0:
            debug('ERROR : Export type MEMORY but could not determine valid size for memory device, exiting.')
            sys.exit(100)
        mem_path = set_up_memory(expsize, exp['storageuuid'])
        if mem_path == None:
            debug('ERROR : Export type MEMORY but failed to set up memory device, exiting')
            sys.exit(101)
        # Set the Memory device export name in the config
        cfg.set(exp['storageuuid'], EXPORTNAME, mem_path)
        '''
	else:
		# Export type is either disk or flash, make sure that we have /dev/sdX
		disk_file = DISK_DIR + exp['storageuuid'] + '/bigfile'
		device_name = scsi_to_device(exp['scsibus'])
		if device_name == None:
			debug('ERROR : Export type is either FLASH or DISK, but no such device found in system. Did you attach the disk/flash device to this ILIO? Exiting!')
			sys.exit(102)
		
		#Do we need to partition sdX?
		if NEEDS_CONFIG:
			debug("Called with 'config' parameter. Partitioning Disk/Flash device requested, attempting to partition %s" % device_name)
			ret = partition(device_name)
			if not ret:
				debug('ERROR : Failed to partition %s, EXITING' % device_name)
				sys.exit(103)
			else:
				debug("Successfully partitioned Disk/Flash device %s" % device_name)

			#if setup_fs(device_name + '1', disk_file) != 0:
			if setup_lvm(device_name + '1', exp['storageuuid']) != 0:
				debug('Setup lvm failed!')
				sys.exit(105)

		# Check whether a partition exists on the disk/flash block device
		disk_partition_number = "1"
		debug("Checking whether Disk/Flash device %s has primary partition %s" %(device_name, disk_partition_number))
		if not check_for_primary_partition(device_name, disk_partition_number):
			debug("ERROR : Disk/Flash device %s does not appear to have partition %s, cannot continue, exiting!" % (device_name, disk_partition_number))
			sys.exit(104)
		else:
			debug(" Verified that Disk/Flash device %s has primary partition %s" % (device_name, disk_partition_number))
		# Set the SCSI device export name in the config
		cfg.set(exp['storageuuid'], EXPORTNAME, disk_file)
		'''

if NEEDS_CONFIG:
    rc = config_disk_flash(exports);
    if rc != 0:
        debug("ERROR: Failed to config disk and flash storage %d" % rc)
        sys.exit(106)

# At this point, we've processed all the exports, so write the IBD config file
# We only write the file if we're running in config mode
if NEEDS_CONFIG:
    debug("Called with 'config' parameter, Writing config to device export file %s" % IBD_CONFIG_FILE)
    # cfgfile = open(IBD_CONFIG_FILE, 'w')
    # cfg.write(cfgfile)
    # cfgfile.close()
    reset_ibdserver_config()
    config_support_server()

else:
    debug("Called with 'start' parameter, will look for an existing device export file %s" % IBD_CONFIG_FILE)

# if rebuild (hypervisor replaced case)
if NEEDS_CONFIG and NEEDS_REBUILD:
    debug("Rebuilding service vm exports... ")
    if os.system('/bin/ibdserver >> %s 2>&1' % (LOG_FILENAME)) != 0:
        debug('ERROR : Failed to initialize export server from config file %s' % IBD_CONFIG_FILE)
        sys.exit(103)
    repair_exports = get_compound_device_export_config()
    if not repair_exports:
        debug("ERROR : REBUILT is set, but unable to get original export info")
        sys.exit(110)
    for export_item in repair_exports:
        export_item_base64 = base64.urlsafe_b64encode(export_item)
        cmd_str = 'python ' + CMD_AGGCREATE + " " + export_item_base64
        rc, msg = runcmd(cmd_str, print_ret=True)
        if rc != 0:
            debug("ERROR : %s failed!" % cmd_str)
            sys.exit(110)

else:
    if os.system('python /opt/milio/atlas/roles/aggregate/agexport.pyc -e') != 0:
        debug('ERROR : Can not export memory files.')
        sys.exit(93)

# Check if we have the IBD export config file
debug("Checking for existence of device export file %s" % IBD_CONFIG_FILE)
if not os.path.isfile(IBD_CONFIG_FILE):
    debug("ERROR : Cannot find device export file %s, exiting with error!" % IBD_CONFIG_FILE)
    sys.exit(104)
else:
    debug("Found existing device export file %s, will attempt to use this file to export devices" % IBD_CONFIG_FILE)

# Now start ibd-server with our new/existing config file
if os.system('/bin/ibdserver >> %s 2>&1' % (LOG_FILENAME)) != 0:
    debug('ERROR : Failed to initialize export server from config file %s' % IBD_CONFIG_FILE)
    sys.exit(103)

# If we got here, ibd-server is supposed to have started up. Check whether it's really running.
sleepsecs = 3
debug('Waiting %s seconds for device export server to fully start up...' % str(sleepsecs))
time.sleep(sleepsecs)
debug('Checking if device export server is running...')
if os.system('ps aux | grep ibdserver | grep -v grep') != 0:
    debug('ERROR : Device export server from config file %s does NOT seem to be running! Exiting!' % IBD_CONFIG_FILE)
    sys.exit(104)

debug('Verified that device export server is running.')
debug('Aggregate node was successfully set up')
debug("==== END AGG NODE CONFIG/START : Successful! ==== ")
