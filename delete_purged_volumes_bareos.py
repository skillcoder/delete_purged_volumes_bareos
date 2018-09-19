#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import re
import os
import sys
import getopt
import traceback
import time
import types
from datetime import datetime
from subprocess import Popen, PIPE

# Config:
import MySQLdb
# TODO: Support PostgreSQL
#import psycopg2

dry_run = True
#dry_run = True
is_debug = False
my_catalog_name = 'MyCatalog'
my_sd_device_name = 'dev-backup'
sd_conf, storages_conf, dir_conf = ('/usr/local/etc/bareos/bareos-sd.conf', '/usr/local/etc/bareos/bareos-dir.d/storages.conf', '/usr/local/etc/bareos/bareos-dir.conf')
levels = {'I': 'incr', 'D': 'diff', 'F': 'full'}

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DARKGRAY = '\033[90m'

def print_color(color, text):
    if text is None:
        return
    if ISCOLOR:
        print(color + text + bcolors.ENDC)
    else:
        print(text)


def parse_vol(volume):
    """Parses volume with bls and returns jobname and timestamp of job."""
    cmd = ['timeout', '0.09', 'bls', '-jv', volume]
    d = dict(os.environ)
    d['LC_ALL'] = '"en_EN.UTF-8"'
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, env=d)
    out, err = p.communicate()
    out = str(out)
    # print(out)
    vol = os.path.basename(volume)
    try:
        ji = re.search('\nJobId\s+:\s(\d+)\n', out).group(1)
        cn = re.search('\nClientName\s+:\s(.*?)\n', out).group(1)
        fn = re.search('\nFileSet\s+:\s(.*?)\n', out).group(1)
        jl = re.search('\nJobLevel\s+:\s(.*?)\n', out).group(1)
        ti = re.search('\nDate written\s+:\s(.*?)\n', out).group(1)
    except Exception as inst:
        print_color(bcolors.WARNING, "NEED!!! Deleting volume, because no metadata found: %s" % vol)
        print_color(bcolors.DARKGRAY, "sudo -u bareos rm %s" % volume)
        return None
    dt = datetime.strptime(ti, '%d-%b-%Y %H:%M')
    ts = time.mktime(dt.timetuple())
    lvl = levels[jl]
    print('{5:<6} {0:<50} {1:<5} {2:<25} {3:<18} {4:<15}'.format(vol, lvl, cn, ti, fn, ji))
    return (cn, fn, ts, jl, ji, vol)


def find_mount_point(path):
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path


def build_volpath(volname, storagename, sd_conf_parsed, storages_conf_parsed):
    """Looks in config files for device path and returns devicename joined with the volname."""
    for storage in storages_conf_parsed:
        if storagename == storage['Name']:
            devicename = storage['Device']
            for device in sd_conf_parsed:
                if devicename == device['Name']:
                    volpath = os.path.join(device['Archive Device'], volname)
                    if not find_mount_point(device["Archive Device"]) == "/":
                        return volpath


def parse_conf(lines):
    parsed = []
    obj = None
    nested_ignore = False
    for line in lines:
        line, hash, comment = line.partition('#')
        line = line.strip()
        if not line:
            continue
        m = re.match(r'(\w+)\s*{', line)
        if m:
            # Start a new object
            if obj is not None:
                # Ignore Nested objects
                nested_ignore = True
                continue

            obj = {'thing': m.group(1)}
            parsed.append(obj)
            continue
        m = re.match(r'\s*}', line)
        if m:
            # End an object
            obj = None
            continue
        if nested_ignore:
            continue
        m = re.match(r'\s*([^=]+)\s*=\s*(.*)$', line)
        if m:
            # An attribute
            key, value = m.groups()
            if '"' in value:
                v = re.match(r'"(.*)"', value)
                obj[key.strip()] = v.group(1)
            else:
                obj[key.strip()] = value.rstrip(';')
            continue
    return parsed

def get_config_block(block_name, item_name, conf_parsed):
    for item in conf_parsed:
        if item['thing'] == block_name and item['Name'] == item_name:
            return item

def format_exception(e):
    """Usage: except Exception as e:
                  log.error(format_exception(e)) """
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))
    exception_str = 'Traceback (most recent call last):\n'
    exception_str += ''.join(exception_list)
    exception_str = exception_str[:-1]  # Removing the last \n
    return exception_str


def vols2str(vols):
    s = ''
    #print('[%d] %s' % (len(vols), type(vols)))
    if isinstance(vols, (dict)):
        vols = [vols]

    if len(vols) == 0:
        s = '\t( EMPTY )\n'
    else:
        for vol in vols:
            s = s + '\t{0:<50} {1:<11}\n'.format(vol['volumename'], vol['jobtdate'])
    return s


def debug(message):
    if is_debug:
        print(message)

def del_backups(remove_backup):
    """Deletes list of backups from disk and catalog"""
    for volpath in remove_backup:
        volname = os.path.basename(volpath)
        print('Deleting %s' % volname)
        print('         %s' % volpath)
        if not dry_run:
            try:
                os.remove(volpath)
            except:
                print('Already deleted vol %s' % volpath)
            p1 = Popen(['echo', 'delete volume=%s yes' % volname], stdout=PIPE)
            p2 = Popen(['bconsole'], stdin=p1.stdout, stdout=PIPE)
            p1.stdout.close()
            out, err = p2.communicate()
            print_color(bcolors.DARKGRAY, out)
            print_color(bcolors.WARNING, err)
            print('')

def bconsole_purge_volume(volname):
    """Force PURGE volume in catalog"""
    print('Pruning %s' % volname)
    if not dry_run:
        p1 = Popen(['echo', 'purge volume=%s yes' % volname], stdout=PIPE)
        p2 = Popen(['bconsole'], stdin=p1.stdout, stdout=PIPE)
        p1.stdout.close()
        out, err = p2.communicate()
        print_color(bcolors.DARKGRAY, out)
        print_color(bcolors.WARNING, err)
        print('')

def clear_file_not_from_catalog(backup_dir):
    """Deleting volumes that are not present in the catalog"""
    print('Checking for volumes that are not present in the catalog: %s' % backup_dir)
    onlyfiles = [f for f in os.listdir(backup_dir) if os.path.isfile(os.path.join(backup_dir, f))]
    for volpath in onlyfiles:
        #echo "list volume=$i" | bconsole | if grep --quiet "No results to list"; then
        #   echo "$i is ready to be deleted"
        volname = os.path.basename(volpath)
        #volname = 'not_exists_in_catalog_volume'
        print('%s' % volname)
        p1 = Popen(['echo', 'list volume=%s' % volname], bufsize=-1, stdout=PIPE)
        p2 = Popen(['bconsole'], bufsize=-1, stdin=p1.stdout, stdout=PIPE)
        p1.stdout.close()
        out, err = p2.communicate()
        m = re.match(r'No results to list', out)
        if m:
            print_color(bcolors.FAIL, "FILE NOT FOUND IN CATALOG")
            print_color(bcolors.WARNING, out)
            print_color(bcolors.DARKGRAY, 'sudo -u bareos rm '+volpath)
            print('DRYRUN IT')
            exit(0)
            print('Deleting unacatalog file: %s' % volname)
            if not dry_run:
                try:
                    os.remove(volpath)
                except:
                    print('Already deleted vol %s' % volpath)
                    

#######################
# START PROGRAMM HERE #
#######################

BINROOT = os.path.abspath(os.path.dirname(sys.argv[0]))

ISCOLOR = False
if os.environ.get('TERM', '') == 'xterm':
    ISCOLOR = True

# Checking if services are up
services = ['bareos-dir']
for x in services:
    p = Popen(['service', x, 'status'], stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    out = out.decode("utf-8").strip()
    # bareos_dir is running as pid 
    #print (out)
    #pattern = re.compile(x+' is running as (\d+)')
    if not re.search(' is running as pid (\d+)', out):
        print("Exiting, because dependent services ["+x+"] are down.")
        sys.exit()


with open (dir_conf, 'r') as f:
    dir_conf_parsed = parse_conf(f)
catalog_cfg = get_config_block('Catalog', my_catalog_name, dir_conf_parsed)

db_driver = catalog_cfg['dbdriver']
db_host   = catalog_cfg.get('dbaddress', '')
db_port   = catalog_cfg.get('dbport', 0)
db_user   = catalog_cfg['dbuser']
db_name   = catalog_cfg['dbname']
db_pass   = catalog_cfg['dbpassword']
print("Connecting to %s %s@%s:%d/%s\n" % (db_driver, db_user, db_host, db_port, db_name))
try:
    con = None
    cur = None
    if db_driver == 'mysql':
        con = MySQLdb.connect(db=db_name, user=db_user, passwd=db_pass)
        cur = con.cursor(MySQLdb.cursors.SSDictCursor)
#   TODO: Support PostgreSQL
#    else:
#        con = psycopg2.connect(database=db_name, user=db_user, host=db_host, password=db_pass)
    cur.execute('SELECT distinct m.volumename, s.name AS `storagename`, m.volstatus, j.jobtdate, j.filesetid, j.clientid, j.level, '
                'c.name AS clientname, f.fileset FROM Media m, Storage s, Job j, JobMedia jm, FileSet f, Client c WHERE '
                'm.storageid=s.storageid AND jm.mediaid=m.mediaid AND jm.jobid=j.jobid AND f.filesetid=j.filesetid AND '
                'j.clientid=c.clientid;')
    volumes = cur.fetchall()
    cur.execute('SELECT distinct m.volumename AS volname, s.name AS storagename FROM Media m, Storage s WHERE '
                "m.storageid=s.storageid AND m.volstatus='Purged';")
    purged_vols = cur.fetchall()
except Exception as e:
    print(format_exception(e))
    print "DATABASE unavailable"
    sys.exit()

unpurged_backups = [x for x in volumes if x['volstatus'] != 'Purged']
full_purged = list()
diff_purged = list()
inc_purged = list()
remove_backup = list()

with open (sd_conf, 'r') as f:
    sd_conf_parsed = parse_conf(f)

with open (storages_conf, 'r') as f:
    storages_conf_parsed = parse_conf(f)

#for x in unpurged_backups:
#    print('{0:<50} {1:15} {2:<6} {3:<1}'.format(x['volumename'], x['name'], x['volstatus'], x['level']))

print("Sorting purged volumes to full_purged, diff_purged and inc_purged.\n")
print('{5:<6} {0:<50} {1:<5} {2:<25} {3:<18} {4:<15}'.format('Volume', 'Level', 'Client', 'Created', 'File set', 'JobId'))
print("-----------------------------------------------------------------------------------------------------------------------")
for x in purged_vols:
    volpath = build_volpath(x['volname'], x['storagename'], sd_conf_parsed, storages_conf_parsed)
    try:
        if not os.path.isfile(volpath):
            print("Deleting backup from catalog, because volume doesn't exist anymore: %s" % volpath)
            del_backups([volpath])
            continue
    except:
        print("Skipping this purged volume, because storage device is not mounted.")
        continue
    vol_parsed = parse_vol(volpath)
    #print x['volname']
    if vol_parsed:
        cn, fn, ts, jl, ji, vol = vol_parsed
    else:
        continue
    x1 = {
          'volpath': volpath
        , 'client':  cn
        , 'fileset': fn
        , 'time':    ts
        , 'id':      ji
        , 'vol':     vol
    }
    if jl == 'F':
        full_purged.append(x1)
    elif jl == 'D':
        diff_purged.append(x1)
    elif jl == 'I':
        inc_purged.append(x1)
    else:
        print "UNKNOWN BACKUP LVL"

print("\n\nDeciding which purged full vols to delete\n")
for vol in full_purged:
    volpath     = vol['volpath']
    name        = vol['vol']
    backup_time = vol['time']
    cn          = vol['client']
    fn          = vol['fileset']
    debug('{1:<6} {0:<50}'.format(name, vol['id']))
    newer_full_backups = [x3 for x3 in unpurged_backups if x3['level'] == 'F' and x3['jobtdate'] > backup_time and cn == x3['clientname'] and fn == x3['fileset']]
    debug('newer_full_backups\n%s' % vols2str(newer_full_backups))
    '''
    0 volumename
    1 storagename
    2 volstatus
    3 jobtdate
    4 filesetid
    5 clientid
    6 level
    7 clientname
    8 fileset
    '''
    our_full_backup = [x3 for x3 in volumes if x3['volumename'] == name ]

    all_full_backups = [x3 for x3 in full_purged if x3['time'] > backup_time and cn == x3['client'] and fn == x3['fileset']]
    #debug('all_full_backups\n%s' % vols2str(all_full_backups))

    if len(newer_full_backups) == 0 and len(all_full_backups) == 0:
        print("Skipping and not removing {0}, because it's the newest full backup.".format(name))
        continue
    if len(our_full_backup) == 0:
        print("Remove {0}, because it not found in catalog and NOT the only one newest full backup".format(name))
        remove_backup.append(volpath)
        continue

    next_full_backup = min(newer_full_backups, key=lambda x: x['jobtdate'])
    debug('next_full_backup\n%s' % vols2str(next_full_backup))

    newer_full_diff_backups = [x3 for x3 in unpurged_backups if x3['level'] in ['F', 'D'] and x3['jobtdate'] > backup_time and cn == x3['clientname'] and fn == x3['fileset']]
    debug('newer_full_diff_backups\n%s' % vols2str(newer_full_diff_backups))

    next_full_diff_backup = min(newer_full_diff_backups, key=lambda x: x['jobtdate'])
    debug('next_full_diff_backup\n%s' % vols2str(next_full_diff_backup))

    inc_backups = [x3 for x3 in unpurged_backups if x3['level'] == 'I' and x3['jobtdate'] > backup_time and x3['jobtdate'] < next_full_diff_backup['jobtdate'] and cn == x3['clientname'] and fn == x3['fileset']]
    debug('inc_backups\n%s' % vols2str(inc_backups))

    diff_backups = [x3 for x3 in unpurged_backups if x3['level'] == 'D' and x3['jobtdate'] > backup_time and x3['jobtdate'] < next_full_backup['jobtdate'] and cn == x3['clientname'] and fn == x3['fileset']]
    debug('diff_backups\n%s' % vols2str(diff_backups))

    full_backups = [x3 for x3 in unpurged_backups if x3['level'] == 'F' and cn == x3['clientname'] and fn == x3['fileset']]
    debug('full_backups\n%s' % vols2str(full_backups))

    if len(inc_backups) > 0:
        print('Not removing {0}, because there are still incremental backups dependent on it.'.format(name))
        print('inc_backups\n%s' % vols2str(inc_backups))
        continue
    if len(diff_backups) > 0:
        print('Not removing {0}, because there are still diff backups dependent on it.'.format(name))
        print('diff_backups\n%s' % vols2str(diff_backups))
        continue
    if len(full_backups) < 1:
        print('Not removing {0}, because we have less than 1 full backups in total.'.format(name))
        print('full_backups\n%s' % vols2str(full_backups))
        continue
    if len(all_full_backups)+len(full_backups) < 4:
        print('Not removing {0}, because we have less than 4 full backups newer this.'.format(name))
        print('full_backups\n%s' % vols2str(full_backups))
        continue
    remove_backup.append(volpath)

print("\n\nDeciding which purged incremental vols to delete")
for vol in inc_purged:
    volpath     = vol['volpath']
    name        = vol['vol']
    backup_time = vol['time']
    cn          = vol['client']
    fn          = vol['fileset']
    debug('{1:<6} {0:<50}'.format(name, vol['id']))

    newer_full_diff_backups = [x3 for x3 in unpurged_backups if x3['level'] in ['F', 'D'] and x3['jobtdate'] > backup_time and cn == x3['clientname'] and fn == x3['fileset']]
    debug('newer_full_diff_backups\n%s' % vols2str(newer_full_diff_backups))

    older_full_diff_backups = [x3 for x3 in unpurged_backups if x3['level'] in ['F', 'D'] and x3['jobtdate'] < backup_time and cn == x3['clientname'] and fn == x3['fileset']]
    debug('older_full_diff_backups\n%s' % vols2str(older_full_diff_backups))

    inc_backups = list()
    for x3 in unpurged_backups:
        inc_filter = [x3['level'] == 'I', cn == x3['clientname'] and fn == x3['fileset']]
        if newer_full_diff_backups:
            next_full_backup = min(newer_full_diff_backups, key=lambda x: x['jobtdate'])
            inc_filter.append(x3['jobtdate'] < next_full_backup['jobtdate'])
        if older_full_diff_backups:
            prev_full_backup = max(older_full_diff_backups, key=lambda x: x['jobtdate'])
            inc_filter.append(x3['jobtdate'] > prev_full_backup['jobtdate'])
        if all(inc_filter):
            inc_backups.append(x3)
    debug('inc_backups\n%s' % vols2str(inc_backups))

    if len(inc_backups) > 0:
        print('Not removing {0}, because there are still chained inc backups that are not purged.'.format(name))
        print('inc_backups\n%s' % vols2str(inc_backups))
        continue
    remove_backup.append(volpath)

print("\n\nDeciding which purged diff vols to delete")
for vol in diff_purged:
    volpath     = vol['volpath']
    name        = vol['vol']
    backup_time = vol['time']
    cn          = vol['client']
    fn          = vol['fileset']
    debug('{1:<6} {0:<50}'.format(name, vol['id']))
    #next_full_backup = min([x3 for x3 in unpurged_backups if x3['level'] == 'F' and x3['jobtdate'] > backup_time and cn == x3['clientname'] and fn == x3['fileset']], key=lambda x: x['jobtdate'])
    newer_full_diff_backups = [x3 for x3 in unpurged_backups if x3['level'] in ['F', 'D'] and x3['jobtdate'] > backup_time and cn == x3['clientname'] and fn == x3['fileset']]
    debug('newer_full_diff_backups %s' % vols2str(newer_full_diff_backups))
    if len(newer_full_diff_backups) == 0:
        print('Not removing {0}, because there its latest diff backups.'.format(name))
        continue

    next_full_diff_backup = min(newer_full_diff_backups, key=lambda x: x['jobtdate'])
    debug('next_full_diff_backup %s' % vols2str(next_full_diff_backup))

    inc_backups = [x3 for x3 in unpurged_backups if x3['level'] == 'I' and x3['jobtdate'] > backup_time and x3['jobtdate'] < next_full_diff_backup['jobtdate'] and cn == x3['clientname'] and fn == x3['fileset']]
    debug('inc_backups %s' % vols2str(inc_backups))

    diff_backups = [x3 for x3 in unpurged_backups if x3['level'] == 'D' and cn == x3['clientname'] and fn == x3['fileset']]
    if len(inc_backups) > 0:
        print('Not removing {0}, because there are still incremental backups dependent on it.'.format(name))
        print('inc_backups %s' % vols2str(inc_backups))
        continue
    '''
    if len(diff_backups) < 1:
        print('Not removing {0}, because we have less than 1 full backups in total.'.format(name))
        continue
    '''
    remove_backup.append(volpath)

print("\n\nDecisions made. Initating deletion.")
del_backups(remove_backup)

#print("\n\nDeleting volumes that are not present in the catalog")
#sd_device_cfg = get_config_block('Device', my_sd_device_name, sd_conf_parsed)
#clear_file_not_from_catalog(sd_device_cfg['Archive Device'])

#    SELECT MediaId, VolumeName, VolBytes, LastWritten, VolStatus FROM Media WHERE LastWritten = '0000-00-00 00:00:00' AND VolStatus = 'Used';
try:
    cur.execute('SELECT MediaId, VolumeName, VolBytes, FirstWritten, LabelDate, InitialWrite, LastWritten, VolStatus '
                'FROM Media WHERE '
                'LastWritten = "0000-00-00 00:00:00" AND VolStatus = "Used" '
                'AND DATE_ADD(LabelDate, INTERVAL 1 DAY) < NOW() AND VolBytes < 10240;')
    volumes = cur.fetchall()
    cur.execute('SELECT MediaId, VolumeName, VolBytes, FirstWritten, LabelDate, InitialWrite, LastWritten, VolStatus '
                'FROM Media WHERE '
                'Recycle = 1;')
    recycles = cur.fetchall()
except Exception as e:
    print(format_exception(e))
    print "DATABASE unavailable"
    sys.exit()

if len(volumes):
    print("\nWe have some failed volumes (Used but never set Purged cuz LastWritten = 0000-00-00 00:00:00)")
    print("Run in bconsole:\npurge yes volume=<VolumeName>")
    print('{0:<50} {1:<8} {2:<19}'.format('VolumeName', 'VolBytes', 'LabelDate'))
    print("-----------------------------------------------------------------------------------------------------------------------")
    for vol in volumes:
        print('{0:<50} {1:<8} {2}'.format(vol['VolumeName'], vol['VolBytes'], vol['LabelDate']))
        if vol['VolBytes'] < 260:
            bconsole_purge_volume(vol['VolumeName'])

if len(recycles):
    print("\nWe have recycles=YES volumes, you must make it recycles=NO")
    print("After update you configs, run in bconsole:\nupdate volume\nSelect 14: All Volumes from all Pools")
    print('{0:<50} {1:<8} {2:<19}'.format('VolumeName', 'VolBytes', 'LabelDate'))
    print("-----------------------------------------------------------------------------------------------------------------------")
    for vol in recycles:
        print('{0:<50} {1:<8} {2}'.format(vol['VolumeName'], vol['VolBytes'], vol['LabelDate']))


