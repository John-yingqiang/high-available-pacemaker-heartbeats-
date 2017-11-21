#!/usr/bin/python

import os
import sys, tempfile, re
#import logging
import time
import base64
from subprocess import *

sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *

wvm_hostname=""
eth0_mode=""
eth0_ip_address=""
eth0_netmask=""
eth0_gateway=""
eth1_mode=""
eth1_ip_address=""
eth1_netmask=""
eth1_gateway=""
eth2_mode=""
eth2_ip_address=""
eth2_netmask=""
eth2_gateway=""
eth3_mode=""
eth3_ip_address=""
eth3_netmask=""
eth3_gateway=""
wvm_dns=""
wvm_timezone=""

# Path to main role directory
ROLE_DIR="/opt/milio/atlas/roles"

# ROLE - witness node
WITNESS_ROLE_DIR="witness"
WITNESS_CONFIG_SCRIPT="witnessstart config"
WITNESS_START_SCRIPT="witnessstart start"

# Special characters
HOSTNAME_SPECIAL_CH = [' ', '_']

LOG_FILENAME = '/var/log/usx-tiebreaker-bootstrap.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename='/var/log/usx-tiebreaker-bootstrap.log',level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))
'''

def info(*args):
    msg = " ".join([str(x) for x in args])
    print >> sys.stderr, msg

def runcmd(cmd, print_ret=False, lines=False, input_string=None):
    if print_ret: debug('Running: %s' % cmd)
    try:
        tmpfile = tempfile.TemporaryFile()
        p = Popen([cmd], stdin=PIPE, stdout=tmpfile, stderr=STDOUT, shell=True, close_fds=False)
        out, err = p.communicate(input_string)
        status = p.returncode
        tmpfile.flush()
        tmpfile.seek(0)
        out = tmpfile.read()
        tmpfile.close()
        if lines and out:
            out = [line for line in out.split("\n") if line != '']
        if print_ret and out: debug(' -> %s: %s' % (status, out))
        return (status, out)
    except OSError:
        return (127, 'OSError')

def check_host_name(hostname):
    ret = hostname
    
    for spch in HOSTNAME_SPECIAL_CH:
        if spch in ret:
            debug('WARNING! host name contains special character "%s"' % spch)
            ret = ret.replace(spch, "")
    
    debug('Replace illegal host name "%s" with new host name: "%s"' % (hostname, ret))
    return ret

def check_hypervisor_type():
        hypervisor_type=""
        ret, output = runcmd('dmidecode -s system-manufacturer', print_ret = False)

        if (ret != 0) or (output is None) or (not output) or (len(output) <= 0):
                debug('WARNING could not get hypervisor_type from dmidecode. Checking for Xen...')
                if os.path.exists('/dev/xvda') == True:
                        hypervisor_type='Xen'
                elif os.path.exists('/dev/sda') == True:
                        hypervisor_type='VMware'
        else:
                output=output.strip()
                if 'Microsoft' in output:
                        hypervisor_type='hyper-v'
                elif 'VMware' in output:
                        hypervisor_type='VMware'
                elif 'Xen' in output:
                        hypervisor_type='Xen'
                else:
                        debug('WARNING do not support hypervisor_type %s' % output)

        return hypervisor_type

#
### Based on  whether the USX Node has already been configured
# or not, figure out the correct script which we need to run.
# Returns: path to script to execute based on role and config status
#       Python 'None' if we could not determine which script to run
#
def build_path_to_wvm_script(needs_configuring=True):
    scriptpath = None
    try:
        if needs_configuring:
            scriptpath = os.path.join(ROLE_DIR, WITNESS_ROLE_DIR, WITNESS_CONFIG_SCRIPT)
        else:
            scriptpath = os.path.join(ROLE_DIR, WITNESS_ROLE_DIR, WITNESS_START_SCRIPT)
    except:
        debug('ERROR : Exception building path to script to run. Exception was: %s' % sys.exc_info()[0])
        scriptpath = None
    if scriptpath is None:
        debug('ERROR : Could not determine the location of the configuration script. NOT CONFIGURING')
        return None

    return scriptpath

#
# Appends py or pyc to the given script path, then tries to find either
# the py or the pyc file on the system. It prefers to run pyc's over pys
# If either is found on the system, it returns scriptname+'py[c]', and
# if neither a py nor a pyc file is found on the system, it returns 
# the python null object None.
#
def find_runnable_pyfile(scriptname):
    try:
        if (scriptname is None) or (not scriptname):
            debug('ERROR : Got invalid path when trying to find runnable script. Returning None.')
            return None

        pyfile = scriptname + '.py'
        pycfile = scriptname + '.pyc'

        #debug('pycfile = %s, pyfile = %s' % (pycfile, pyfile))
    
        # Find out if we need to run the pyc or the py script
        # We prefer to run the pyc over the py
        if os.path.isfile(pycfile):
            debug('Found compiled version %s' % pycfile)
            return pycfile
        elif os.path.isfile(pyfile):
            debug('Found non-compiled %s' % pyfile)
            return pyfile
        else:
            debug('ERROR : Could not find which script to run, cannot run anything')
            return None
    except:
        debug('ERROR : exception trying to find runnable file for %s :: Exception was %s' %(scriptname, sys.exc_info()[0]))
        return None

# From a given script path, check whether we have the corresponding pyc or py
# file, and run it. Priority is given to pyc files over py files. 
#
# This function will also handle script arguments passed in.
# It returns the system return code (usually 0) of the script
# to be run if it ran successfully, non-zero otherwise.
def runscript(scriptpath):
    if scriptpath is None:
        debug('ERROR : Script path to execute is Null, nothing to execute! Error!')
        return(120)

    scriptpath = scriptpath.strip()
    debug('stripped script path received: %s' % scriptpath)

    if not scriptpath:
        debug('ERROR : Script path after trimming is empty, nothing to execute! Error!')
        return(120)

    scriptname = ''
    scriptparams = ''
    cmd = ''

    # If we have been passed a path with spaces, we assume that the first
    # part of the string before the first whitespace is the script name, 
    # and the rest of the string following the first space character are
    # command line arguments to the script.
    if ' ' in scriptpath:
        scriptname, scriptparams = scriptpath.split(None, 1)
    else:
        scriptname = scriptpath

    pyfile = find_runnable_pyfile(scriptname)

    if (pyfile is None) or (not pyfile):
        debug('ERROR : Cannot determine proper script name to execute! Not executing anything')
        return 121

    #debug('pycfile = %s, pyfile = %s' % (pycfile, pyfile))
    cmd = 'python ' + pyfile
    

    cmd += (' ' + scriptparams)

    debug('cmd = %s' % cmd)
    ret = os.system(cmd)
    return(ret)

def valid_ip_address(addr):
    try:
        tmp = addr.split('.')
        if len(tmp) != 4:
            return False
        for x in tmp:
            v = int(x)
            if v > 255 or v < 0:
                return False
    except:
        debug("WARN : There was an exception validating the IP.")
        return False
    
    return True

#
# Check whether a given IPv4 extended netmask is a valid netmask.
#
# This fix was put in for TISILIO-3738.
#
# NOTE: This function DOES NOT check whether a given combination of IP address
# and netmask is a valid combination; it only checks for a valid netmask.
# 
# For more info on what constitues a valid netmask, please read:
#     http://www.gadgetwiz.com/network/netmask.html
# 
# Parameters:
#     netmask : netmask to be checked, in extended (x.y.z.a) format
# 
# Returns:
#     0     :     Given netmask is a valid netmask
#     !=0    :    Given netmask is invalid, or there was an error checking given 
#             netmask.
def validate_netmask(netmask):
    if netmask is None or not netmask:
        debug("WARN : Check netmask : Null or empty netmask received. Cannot check netmask.")
        return 1
    try:
        # Split the given netmask into octets
        octets = netmask.split('.')
        if len(octets) != 4:
            debug("WARN : Check netmask : Decomposing given netmask "+netmask+" into octets yielded "+str(len(octets)) + " octets, but we expect exactly 4 octets.")
            return 2

        # OK, we have the expected number of octets. Now convert the given 
        # netmask into a single integer
        addr = 0
        for octet in octets:
            addr = addr * 256 + int(octet)

        # addr is now a single integer representing the given netmask.
        # We now convert addr into binary, and discard the leading "0b"
        binaddr = bin(addr)[2:]

        # This is the key: Now we check if the binary representation of addr
        # contains the string "01". A valid netmask will ONLY have 0's on the
        # right hand side; there is never a 0 followed by 1 in a valid netmask
        strpos = binaddr.find("01")

        if strpos >= 0:
            debug("WARN : Check netmask : Netmask "+netmask+" is INVALID!")
            return 3

        # If we got here, we have a valid netmask.
#         debug("INFO : Check netmask : Netmask "+netmask+" is a valid netmask, all OK.")
        return 0

    except:
        debug("WARN : Check netmask : There was an exception validating the netmask.")
        return 4

def check_ip_conflict(interface, ip):
    rc = 0
    if not os.path.isfile("/usr/sbin/arping"):
        debug("arping not found, skip ip conflict check.")
        return 0

    ipup_cmd_str = "ifconfig  %s up" % (interface)
    rc, msg = runcmd(ipup_cmd_str, print_ret=True)
    if rc == 0:
        debug("ifconfig %s up is successful!" % ( interface))
        debug(msg)
    else:
        debug("error on ifconfig %s up!" % ( interface))
        debug(msg)
        return rc

    # arping wait time less than 1 second doesn't work, bug in arping?
    # TODO: arping 2.15 fixed this issue, should upgrade arping.
    cmd_str = "/usr/sbin/arping -r -0 -w 1000000 -c 2 -i %s %s" % (interface, ip)
    rc, msg = runcmd(cmd_str, print_ret=True)
    if rc == 0:
        debug("ip conflict detected: %s already exist on interface: %s!" % (ip, interface))
        debug(msg)
        rc = 1
    else:
        debug("ip: %s not found on interface: %s. rc: %d." % (ip, interface, rc))
        debug(msg)
        rc = 0

    return rc

def exit_on_ip_conflict(interface, ip):
    rc = check_ip_conflict(interface, ip)
    if rc != 0:
        msgstr = "ERROR : IP conflict detected for interface: %s ip: %s, BOOTSTRAP ABORTED!" % (interface, ip)
        debug(msgstr)
        sys.exit(64)
    return 0

def get_user_input_settings():
    global wvm_hostname
    global eth0_mode
    global eth0_ip_address
    global eth0_netmask
    global eth0_gateway
    global eth1_mode
    global eth1_ip_address
    global eth1_netmask
    global eth1_gateway
    global eth2_mode
    global eth2_ip_address
    global eth2_netmask
    global eth2_gateway
    global eth3_mode
    global eth3_ip_address
    global eth3_netmask
    global eth3_gateway
    global wvm_dns
    global wvm_timezone

    if wvm_hostname == "":
        print 'Enter Hostname: ',
        wvm_hostname = raw_input('Enter Hostname: ')

    if eth0_ip_address == "":
        while True:
            print 'Enter a valid IP Address: ',
            eth0_ip_address = raw_input('Enter a valid IP Address: ')
            if valid_ip_address(eth0_ip_address):
                print 'eth0 IP address: %s' % eth0_ip_address
                break
            else:
                print 'Invalid input! Please try again.'
                    
    if eth0_netmask == "" and eth0_ip_address != "DHCP":
        while True:
            print 'Enter a valid Netmask: ',
            eth0_netmask = raw_input('Enter a valid Netmask: ')
            if validate_netmask(eth0_netmask) == 0:
                print 'eth0 Netmask: %s' % eth0_netmask
                break
            else:
                print 'Invalid input! Please try again.'
                
    if eth0_gateway == "" and eth0_ip_address != "DHCP":
        # empty value is acceptable.
        while True:
            print 'Enter Gateway, press "Enter" to leave it empty: ',
            eth0_gateway = raw_input('Enter Gateway, press "Enter" to leave it empty: ')
            if eth0_gateway == "" or valid_ip_address(eth0_gateway):
                print 'eth0 gateway: %s' % eth0_gateway
                break
            else:
                print 'Invalid input! Please try again.'
                
    if wvm_dns == "":
        print 'Enter DNS Server: ',
        wvm_dns = raw_input('Enter DNS Server: ')
        if wvm_dns != "":
            print 'DNS server: %s' % wvm_dns
    
    if wvm_timezone == "":
        print 'Enter Timezone (America/Los_Angeles): ',
        wvm_timezone = raw_input('Enter Timezone (America/Los_Angeles): ')
        if wvm_timezone == "":
            wvm_timezone = 'America/Los_Angeles'

def ibdserver_start():
    ret = 0
    if os.system('/bin/ibdserver >> %s 2>&1' % (LOG_FILENAME)) != 0:
        debug('ERROR : Failed to start ibdserver')
        ret = 103

    # If we got here, ibd-server is supposed to have started up. Check whether it's really running.
    sleepsecs=3
    debug('Waiting %s seconds for ibdserver to fully start up...' % str(sleepsecs))
    time.sleep(sleepsecs)
    debug('Checking if ibdserver is running...')
    if os.system('ps aux | grep ibdserver | grep -v grep') != 0:
        debug('ERROR : Device ibdserver does NOT seem to be running! Exiting!')
        ret = 104

    return ret

#
# START HERE
#

if os.path.exists('/usr/share/ilio/configured') == True:
    debug('======= START USX Tiebreaker Bootstrap =======')
    debug('This USX node is already configured as USX Tiebreaker')
    rc = ibdserver_start()
    if rc != 0:
        debug("ERROR: Failed to start ibdserver %s" % rc)
        sys.exit(rc)
    else:
        debug('Start script for USX Tiebreaker SUCCEEDED. ')
        debug('======= END USX Tiebreaker bootstrap =======')
        sys.exit(0)

debug("Configure USX Node...")
selection = ''
while True:
    print 'Do you want to configure the USX node as an USX Tiebreaker (y/n)?',
    selection = raw_input('Do you want to configure the USX node as an USX Tiebreaker (y/n)?').lower()
    if selection == 'y' or selection == 'n' or selection  == 'yes' or selection == 'no':
        break
if selection == 'n' or selection == 'no': # configure as USX Tiebreaker not required
    sys.exit(0)

hypervisor_type = check_hypervisor_type()
# debug('hypervisor_type is %s' % hypervisor_type)

# Interactively get user input to configure this vm
get_user_input_settings()

if eth0_ip_address:
    exit_on_ip_conflict("eth0", eth0_ip_address)
    cmd='ilio net add_static --interface=eth0 --address=%s --netmask=%s' % (eth0_ip_address, eth0_netmask)
    if eth0_gateway != '' and eth0_gateway != '0.0.0.0':
        cmd += ' --gateway=' + eth0_gateway
    debug('Configuring eth0 in static mode with IP Address ' + eth0_ip_address)
    ret, msg = runcmd('ilio net remove --interface=eth0', print_ret=True)
    ret, msg = runcmd(cmd, print_ret = True)
    if (ret != 0):
        debug( 'ERROR : Error configuring eth0 in static mode: ' + msg)
        sys.exit(64)
 
    # Write the list of DNS servers to resolv.conf. Max 3 entries.
    if wvm_dns:
        dnsset = wvm_dns.split()
        with open('/etc/resolv.conf', 'w') as f:
            dnsctr = 1
            for dnsentry in dnsset:
                f.write('nameserver '+dnsentry+'\n')
                dnsctr += 1
                if dnsctr >= 4:
                    break

if wvm_hostname != '':
    wvm_hostname = check_host_name(wvm_hostname)
    debug('Setting hostname to %s' % (wvm_hostname))
    f = open('/etc/hostname', 'w')
    f.write('%s' % wvm_hostname +'\n')
    f.flush()
    f.close()
    runcmd('/etc/init.d/hostname.sh stop')
    runcmd('/etc/init.d/hostname.sh start')
 
if wvm_timezone != '':
    runcmd('rm -f /etc/localtime')
    runcmd('ln -s /usr/share/zoneinfo/%s /etc/localtime'%(wvm_timezone))
    f=open('/etc/timezone', 'w')
    f.write('%s' % wvm_timezone)
    f.flush()
    f.close()
    runcmd('service cron restart')

rc = ibdserver_start()
if rc != 0:
    debug("ERROR: Failed to start ibdserver %s" % rc)
    sys.exit(rc)
else:
    debug('Configure script for USX Tiebreaker SUCCEEDED. ')

debug('Setting "Configured" property for this node')
f = open('/usr/share/ilio/configured', 'w')
f.write(' ')
f.flush()
f.close()

sys.exit(0)    
