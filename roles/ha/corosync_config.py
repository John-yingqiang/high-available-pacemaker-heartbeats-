#!/usr/bin/python

import os, sys
import json
import logging
import tempfile
from subprocess import *
import httplib
from ha_util import *

sys.path.insert(0, "/opt/milio/libs/atlas")
from set_multicast_route import set_multicast_routes_for_ha

ATLAS_CONF = "/etc/ilio/atlas.json"
COROSYNC_CONF = "/etc/corosync/corosync.conf"
COROSYNC_AUTHKEY = "/etc/corosync/authkey"
COROSYNC_DEFAULT = "/etc/default/corosync"

JSON_PARSE_EXCEPTION = 10
CORO_NOT_RUNNING = 11
CORO_FETCH_AMC_FAIL = 12
SET_ALIAS_FAILED = 13

corosync_conf_dict = {
	"totem"		: {	
		"version"				: 2,
		"token"					: 3000,
		"token_retransmits_before_loss_const"	: 4,
		"join"					: 60,
		"consensus"				: 3600,
		"vsftype"				: "none",
		"max_messages"				: 20,
		"clear_node_high_bit"			: "yes",
		"secauth"				: "off",
		"threads"				: 0,
		"rrp_mode"				: "none",
		"interface"				: {
			"ringnumber"		: 0,
			"bindnetaddr"		: "10.17.0.0",
			"mcastaddr"		: "226.94.1.7",
			"mcastport"		: 7777
		}
	},

	"quorum"	: {
		"provider"		: "corosync_votequorum",
		"expected_votes"	: 2,
	},

	"amf"		: {
		"mode"			: "disabled"
	},

	"service"	: {
		"ver"			: 2,
		"name"			: "pacemaker"
	},

	"aisexec"	: {
		"user"			: "root",
		"group"			: "root"
	},

	"logging"	: {
		"fileline"		: "off",
		"to_stderr"		: "yes",
		"to_logfile"	: "yes",
		"to_syslog"		: "yes",
		"logfile"		: "/var/log/corosync.log",
		"syslog_facility"	: "daemon",
		"debug"			: "off",
		"timestamp"		: "on",
		"logger_subsys"		: {
			"subsys"	: "AMF",
			"debug"		: "off",
			"tags"		: "enter|leave|trace1|trace2|trace3|trace4|trace6"
		}
	}
}

LOG_FILENAME = '/var/log/usx-atlas-ha.log'
set_log_file(LOG_FILENAME)

def runcmd(
    cmd,
    print_ret=False,
    lines=False,
    input_string=None,
    ):
    if print_ret:
        debug('Running: %s' % cmd)
    try:
        tmpfile = tempfile.TemporaryFile()
        p = Popen(
            [cmd],
            stdin=PIPE,
            stdout=tmpfile,
            stderr=STDOUT,
            shell=True,
            close_fds=True,
            )
        (out, err) = p.communicate(input_string)
        status = p.returncode
        tmpfile.flush()
        tmpfile.seek(0)
        out = tmpfile.read()
        tmpfile.close()

        if lines and out:
            out = [line for line in out.split('\n') if line != '']

        if print_ret:
            debug(' -> %s: %s: %s' % (status, err, out))
        return (status, out)
    except OSError:
        return (127, 'OSError')

def gen_generic(cfile, section_dict, title, dent):
	cfile.write(dent + title +  " {\n")
	for item in section_dict:
		cfile.write(dent + "	" + item + ": ")
		cfile.write(str(section_dict[item]) + '\n')
	cfile.write(dent + "}\n\n")
	return

def gen_totem(cfile):
	title = "totem"
	section_dict = corosync_conf_dict[title]
	cfile.write(title +  " {\n")
	for item in section_dict:
		if item != "interface" :
			cfile.write("	" + item + ": ")
			cfile.write(str(section_dict[item]) + '\n')
			
	sub_title = "interface"
	gen_generic(cfile, corosync_conf_dict[title][sub_title], sub_title, "	")
	cfile.write("}\n\n")
	return


def gen_quorum(cfile):
	title = "quorum"
	gen_generic(cfile, corosync_conf_dict[title], title, "")
	return

def gen_amf(cfile):
	title = "amf"
	gen_generic(cfile, corosync_conf_dict[title], title, "")
	return

def gen_service(cfile):
	title = "service"
	gen_generic(cfile, corosync_conf_dict[title], title, "")
	return

def gen_aisexec(cfile):
	title = "aisexec"
	gen_generic(cfile, corosync_conf_dict[title], title, "")
	return

def gen_logging(cfile):
	title = "logging"
	section_dict = corosync_conf_dict[title]
	cfile.write(title +  " {\n")
	for item in section_dict:
		if item != "logger_subsys" :
			cfile.write("	" + item + ": ")
			cfile.write(str(section_dict[item]) + '\n')
			
	sub_title = "logger_subsys"
	gen_generic(cfile, corosync_conf_dict[title][sub_title], sub_title, "	")
	cfile.write("}\n\n")
	return

section_func = {
	"totem"		: gen_totem,
	"quorum"	: gen_quorum,
	"amf"		: gen_amf,
	"service"	: gen_service,
	"aisexec"	: gen_aisexec,
	"logging"	: gen_logging
}

haconf_dict = None
nics = None
service_ip = None

def configure_corosync():
    global haconf_dict
    global corosync_conf_dict
    global nics
    global service_ip

    ring_dict = haconf_dict.get('ring')
    if ring_dict is None or len(ring_dict) == 0:
	debug('Error getting ring. HA will NOT be enabled for this node')
	return(JSON_PARSE_EXCEPTION)

    authkey_str = haconf_dict.get('authkey')
    if authkey_str is None:
	debug('Error getting authentication key. HA will NOT be enabled for this node')
	return(JSON_PARSE_EXCEPTION)

    interface_dict = corosync_conf_dict["totem"]["interface"]
    interface_dict["mcastaddr"] = ring_dict[0]["multicastip"]
    interface_dict["mcastport"] = ring_dict[0]["multicastport"]
    for nic in nics:
	if nic.get("storagenetwork") is True:
	    pip = nic.get("ipaddress")
	    pmask = nic.get("netmask") 
	    pdevice = nic.get("devicename")
	    break

    # generate bindnetaddr
    pip_list = pip.split('.')
    pmask_list = pmask.split('.')
    bindnetaddr_list = []
    for i in range(len(pip_list)):
	bindnetaddr_list.append(str(int(pip_list[i]) & int(pmask_list[i])))
    interface_dict["bindnetaddr"] = '.'.join(bindnetaddr_list)

    tmp_fname = "/tmp/corosync.conf"
    cfgfile = open(tmp_fname, "w")
    for section in corosync_conf_dict:
	section_func[section](cfgfile)

    cfgfile.close()
    os.rename(tmp_fname, COROSYNC_CONF)

    tmp_fname = "/tmp/authkey"
    cfgfile = open(tmp_fname, "w")
    cfgfile.write(authkey_str)
    cfgfile.close()
    os.rename(tmp_fname, COROSYNC_AUTHKEY)

    tmp_fname = "/tmp/corosync"
    cfgfile = open(tmp_fname, "w")
    cfginfo='# start corosync at boot [yes|no]\nSTART=yes\n'
    cfgfile.write(cfginfo)
    cfgfile.close()
    os.rename(tmp_fname, COROSYNC_DEFAULT)

	# delete the storage network interface alias before corosync start
    cmd_str=""
    if service_ip is not None:
    	(subret, cidrmask) = netmask2cidrmask(pmask)
        if (subret != 0): # convert netmask failed
            debug("config_corosync: convert netmask failed")
            return subret
        cmd_str = (("OCF_ROOT=/usr/lib/ocf/ OCF_RESKEY_ip=%s OCF_RESKEY_cidr_netmask=%s " +  
				   "/usr/lib/ocf/resource.d/heartbeat/IPaddr2 stop") % (service_ip, cidrmask))
        #cmd_str = ('ip addr del %s/%s dev %s' % (service_ip, pmask, pdevice))
        (ret, msg) = runcmd(cmd_str, print_ret=True)

    cmd = 'service corosync restart'
    (pret, msg) = runcmd(cmd, print_ret=True, lines=True)
    # set config to rotate log after corosync starts
    ha_logrotate_conf()
	# add the storage network interface alias after corosync started
    if service_ip is not None:
		(subret, cidrmask) = netmask2cidrmask(pmask)
		if (subret != 0): # convert netmask failed
			debug("config_corosync: convert netmask failed")
			return subret
		cmd_str = (("OCF_ROOT=/usr/lib/ocf/ OCF_RESKEY_ip=%s OCF_RESKEY_cidr_netmask=%s " +  
				   "/usr/lib/ocf/resource.d/heartbeat/IPaddr2 start") % (service_ip, cidrmask))
		#cmd_str = ('ip addr add %s/%s dev %s' % (service_ip, pmask, pdevice))
		(ret, msg) = runcmd(cmd_str, print_ret=True)
		if ret!= 0:
			debug("add alias storage network interface failed!")
			return SET_ALIAS_FAILED
    if pret == 1: # corosync restart return value = 1 if SUCCESSFUL !!! WTF
		debug('Corosync started')
		return(0)
    for line in msg:
		if line.find('OK') >= 0:
		    return 0
    debug('Failed to start corosync')
    return(CORO_NOT_RUNNING)

#########################################################
#		START HERE 				#
#########################################################

try:
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    node_dict = json.loads(s)
    if node_dict is None:
	sys.exit(1)
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
	sys.exit(1)
    if ilio_dict.get('ha') is None:
        sys.exit(0)				# stop if this is not a ha node
except:
    debug('Exception checking whether HA or not, HA will NOT be enabled. Exception was: %s' % sys.exc_info()[0])
    sys.exit(JSON_PARSE_EXCEPTION)

if node_dict.has_key('volumeresources'):
	if node_dict['volumeresources']:
		service_ip = node_dict['volumeresources'][0]['serviceip']

roles = ilio_dict.get('roles')
if roles is None or len(roles) == 0:
    debug('Error getting role information. HA will NOT be enabled for this node')
    sys.exit(JSON_PARSE_EXCEPTION)
role = roles[0]

uuid = ilio_dict.get('uuid')
if uuid is None:
    debug('Error getting Ilio UUID. HA will NOT be enabled for this node.')
    sys.exit(JSON_PARSE_EXCEPTION)

if role == 'VOLUME':
    amcfile = "/usx/inventory/volume/containers/" + uuid + '?composite=true'
else:
    haconf_dict = ilio_dict.get('haconfig')
    if haconf_dict is None:
	debug('Error getting HA information. HA will NOT be enabled for this node')
	sys.exit(JSON_PARSE_EXCEPTION)
    sys.exit(configure_corosync())

(ret, res_data) = ha_query_amc2(LOCAL_AGENT, amcfile, 2)
if ret != 0:
    sys.exit(JSON_PARSE_EXCEPTION)
try:
    node_dict = res_data['data']
    #debug('corosync_config node dict JSON: ', json.dumps(node_dict, sort_keys=True, indent=4, separators=(',', ': ')))
    if node_dict is None:
	debug('Error getting Ilio data. HA will NOT be enabled for this node.')
	sys.exit(JSON_PARSE_EXCEPTION)

    set_multicast_routes_for_ha(node_dict)
    # save the new data
    tmp_fname = '/tmp/new_atlas_conf.json'
    cfgfile = open(tmp_fname, "w")
    node_dict['usx']['ha'] = False
    json.dump(node_dict, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
    cfgfile.close()
    os.rename(tmp_fname, ATLAS_CONF)

    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
	debug('Error getting Ilio information. HA will NOT be enabled for this node.')
	sys.exit(JSON_PARSE_EXCEPTION)
    nics = ilio_dict.get('nics')
    if nics is None:
	debug('Error getting Ilio NICS information. HA will NOT be enabled for this node.')
	sys.exit(JSON_PARSE_EXCEPTION)
    haconf_dict = node_dict.get('haconfig')
    if haconf_dict is None:
	debug('Error getting HA information (no haconfig). HA will NOT be enabled for this node.')
	sys.exit(JSON_PARSE_EXCEPTION)
    sys.exit(configure_corosync())
except ValueError, e:
    debug('Exception checking network info. HA will NOT be enabled. Exception was: ' + str(e))
    sys.exit(JSON_PARSE_EXCEPTION)

