# !/usr/bin/python
import sys
import signal
import json
import traceback
import glob

sys.path.insert(0, "/opt/milio/")
from libs.atlas.cmd import runcmd
from libs.atlas.atl_storage import lvm_free_space

sys.path.insert(0, "/opt/milio/libs/atlas/")
from log import *


def validity_check(dev_name):
    list_match = glob.glob('{}*'.format(dev_name))
    if len(list_match) == 0:
        raise SystemError('There is not have device {}'.format(dev_name))
    elif '{}1'.format(dev_name) in list_match:
        ret, msg = runcmd('pvs', print_ret=True)
        if ret != 0:
            raise SystemError('can\'t get information of pvs')
        if '{}1'.format(dev_name) in msg:
            raise SystemError('has same device in pv list')


def get_vganme_devuuid_by_devtype(dev_type):
    json_data = load_json('/etc/ilio/atlas.json')
    if json_data["roles"][0].lower() not in ['service_vm']:
        raise ValueError('roles is not service vm ')
    for device_detail in json_data['export']:
        if dev_type in device_detail['type'] and device_detail.get('virtual', False) == 'true':
            return device_detail['storageuuid'], device_detail['uuid']
    else:
        raise ValueError('can\'t find correct storage uuid by devcie type')


def load_json(json_file):
    try:
        with open(json_file, 'r') as read_f:
            data = read_f.read()
    except Exception as e:
        raise e
    return json.loads(data)


def partition_dev(dev_name):
    debug('Partitioning disk {}, checking paritions...'.format(dev_name))
    if not check_partition_table(dev_name):
        raise SystemError('check device partition failed')
    clear_out_partitions(dev_name)
    cmd = 'parted -s {} unit s mkpart primary ext3 {} 100%'.format(dev_name, '2048')
    debug('Partitioning disk {}, Creating partition, cmd=%s'.format(dev_name, cmd))
    ret, msg = runcmd(cmd, print_ret=True)
    if ret == 0:
        debug('Partitioning disk {}, Creating partition Succeeded'.format(dev_name))

    else:
        clear_out_partitions(dev_name)
        raise SystemError('ERROR : Failed to create partitions on {}'.format(dev_name))


def check_partition_table(dev_name):
    cmd = 'parted -s {} print'.format(dev_name)
    ret, msg = runcmd(cmd, print_ret=True)
    if ret == 0:
        return True
    if 'unrecognised disk label' in msg:
        cmd = 'parted -s {} mklabel msdos'.format(dev_name)
        ret, msg = runcmd(cmd, print_ret=True)
        if ret == 0:
            return True
        debug('check partition failed with {}'.format(msg))
        return False
    else:
        debug('check partition failed with {}'.format(msg))
        return False


def clear_out_partitions(dev_name):
    for num in range(1, 3):
        clear_out_single_partition(dev_name, num)


def clear_out_single_partition(dev_name, num):
    cmd = 'parted -s {} rm {}'.format(dev_name, num)
    ret, msg = runcmd(cmd, print_ret=True)
    if ret != 0 and 'doesn\'t exist' not in msg:
        SystemError(msg)


def extend_vg(vgname, partition_dev):
    ret, msg = runcmd('/sbin/vgextend {} {}'.format(vgname, partition_dev), print_ret=True)
    if ret != 0:
        raise SystemError('extend vg failed.')


def update_usxmanager_capacity(uuid, sizeG):
    cmd_str = "curl -k -X PUT http://127.0.0.1:8080/usxmanager/usx/inventory/servicevm/exports/" + uuid + '/' + str(
        sizeG)
    ret, msg = runcmd(cmd_str, print_ret=True)
    if ret != 0:
        raise SystemError('Update capcity failed')


def extend_service_vm_capacity(dev_type, dev_name):
    try:
        validity_check(dev_name)
        vganme, dev_uuid = get_vganme_devuuid_by_devtype(dev_type)
        partition_dev(dev_name)
        partition_device_name = '{}1'.format(dev_name)
        extend_vg(vganme, partition_device_name)
        vol_size = lvm_free_space(vganme)
        debug("INFO: Update capacity of {}:{} to {}".format(vganme, dev_uuid, vol_size))
        update_usxmanager_capacity(dev_uuid, vol_size)
    except Exception as e:
        raise e


class UserError(Exception):
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        return repr(self.value)


class UserExitError(UserError):
    pass


class UserInputError(UserError):
    pass


def signal_handler(signal, frame):
    usx_print('\nYou pressed Ctrl+C! Exiting...')
    sys.exit(0)


def usx_print(*args):
    msg = ' '.join([str(arg) for arg in args])
    sys.stdout.write(msg)
    sys.stdout.write('\n')


def dbg_print(*args):
    # print args
    pass


def ag_scsi_hotscan():
    hosts_list = os.popen("ls /sys/class/scsi_host/", 'r', 1).read().split('\n')
    for the_host in hosts_list:
        if len(the_host) == 0:
            continue
        the_hostname = "/sys/class/scsi_host/" + the_host
        the_scanname = the_hostname + "/scan"
        cmd_str = "echo \"- - -\" > " + the_scanname
        runcmd(cmd_str)


def get_vg_list():
    cmd_str = 'vgs --noheadings'
    ret, lines = runcmd(cmd_str, lines=True)
    if ret != 0:
        raise OSError('cannot get vg info.')
    vgs = []
    for line in lines:
        info = line.split()
        vg_name = info[0]
        vgs.append(vg_name)
    if not vgs:
        raise OSError('cannot get vg info.')
    return vgs


def get_vg_dev_list():
    cmd_str = 'pvs --noheadings'
    ret, lines = runcmd(cmd_str, lines=True)
    if ret != 0:
        raise OSError('cannot get vg devices.')
    vg_devices = []
    for line in lines:
        info = line.split()
        dev_name = info[0]
        vg_name = info[1]
        if 'sda' not in dev_name:
            vg_devices.append(dev_name.rstrip('1'))
    if not vg_devices:
        raise OSError('cannot get vg devices.')
    return vg_devices


def get_availible_dev_list():
    cmd_str = 'lsblk -l -n -o NAME,TYPE'
    ret, lines = runcmd(cmd_str, lines=True)
    if ret != 0:
        raise OSError('cannot get availible device for extending.')
    availible_devices = []
    for line in lines:
        info = line.split()
        dev_name = info[0]
        dev_type = info[1]
        if dev_type == 'disk' and (dev_name.startswith('sd') or dev_name.startswith('xvd')) and dev_name not in ['sda',
                                                                                                                 'xvda']:
            availible_devices.append('/dev/{}'.format(dev_name))
    if not availible_devices:
        raise OSError('cannot get availible device for extending.')
    vg_devices = get_vg_dev_list()
    dbg_print(availible_devices, vg_devices)
    availible_devices = [dev for dev in availible_devices if dev not in vg_devices]
    if not availible_devices:
        raise OSError('cannot find any availible device for extending. make sure you have added one.')
    return availible_devices


def diplay_list(lst):
    for index, item in enumerate(lst):
        sys.stdout.write('\n[{}] :\t{}'.format(index, item))
    sys.stdout.write('\n\n')


def interactive_mode(argv):
    usx_print('Entering Interactive Mode:\n')

    availible_devices = get_availible_dev_list()
    usx_print('Please select the device which you want to extend:')
    while True:
        diplay_list(availible_devices)
        the_input = raw_input('Input your index: ')
        try:
            dev_index = int(the_input)
        except:
            continue
        if dev_index in range(len(availible_devices)):
            chosen_device = availible_devices[dev_index]
            break

    usx_print('Please select the type of device which you want to extend:')
    device_types = ['HDD', 'SSD']
    inner_device_types = ['DISK', 'FLASH']
    while True:
        diplay_list(device_types)
        the_input = raw_input('Input your index: ')
        try:
            dev_type_index = int(the_input)
        except:
            continue
        if dev_type_index in range(len(device_types)):
            chosen_device_type = inner_device_types[dev_type_index]
            break

    usx_print('Please confirm the device and it\'s type:')
    usx_print('Device: {}, Type: {}'.format(chosen_device, device_types[dev_type_index]))
    for cnt in range(3):
        the_input = raw_input('Entend ? (Y/N): ')
        if the_input.upper() == 'Y':
            # do extend.
            extend_service_vm_capacity(chosen_device_type, chosen_device)
            break
        elif the_input.upper() == 'N':
            # exit
            raise UserExitError()
        else:
            # exit of loop 3 times and then exit.
            continue
    else:
        usx_print('Input error more than 3 times')
        raise UserExitError()


def script_mode(argv):
    usx_print('Entering Script Mode:\n')
    dev_name = argv[0]
    dev_type = argv[1]
    def_type_map = {
        'HDD': 'DISK',
        'SSD': 'FLASH'
    }
    dev_type = def_type_map.get(dev_type.upper())
    if not dev_type:
        raise UserInputError()
    extend_service_vm_capacity(dev_type, dev_name)


def usage():
    usx_print('\n================================USAGE OF THIS SCRIPT================================\n\n')
    usx_print('[1] Interactive Mode: python /opt/milio/atlas/roles/aggregate/agextend.pyc\n')
    usx_print('[2] Script Mode: python /opt/milio/atlas/roles/aggregate/agextend.pyc DEVNAME DEVTYPE')
    usx_print('----DEVNAME: the path of device which you want to extend.such as /dev/sdc,/dev/sdd...')
    usx_print('----DEVTYPE: the type of device which you want to extend.only support HDD and SSD.')
    usx_print('\t----HDD: HARD DISK DRIVE')
    usx_print('\t----SSD: SOLID STATE DRIVE')


def main():
    try:
        dbg_print(sys.argv)
        ag_scsi_hotscan()
        argv = sys.argv[1:]
        if len(argv) == 2:
            script_mode(argv)
        elif not argv:
            interactive_mode(argv)
        else:
            # usage()
            raise UserInputError()
        ret = 0
        usx_print('Extend disk successfully.')
    except UserInputError:
        usage()
        ret = 1
    except UserExitError:
        ret = 0
    except Exception as e:
        usx_print(e)
        dbg_print(traceback.format_exc())
        ret = 1
    return ret


if __name__ == '__main__':
    dbg_print('Entering Physical Disk Extending Script:')
    signal.signal(signal.SIGINT, signal_handler)
    ret = main()
    sys.exit(ret)
