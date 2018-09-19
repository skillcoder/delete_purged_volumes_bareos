# Delete Purged Volumes Bareos/Bacula
Script for Delete Purged Volumes from Bareos/Bacula Catalog and free disk space  
It removing volumes from catalog that have been marked 'Purged', and delete backup vols from disk  
Deletion rules:  
 * Don't delete full backup if:
   - we have less than 4 fulls
   - we have any of incremental/diff backup dependent on this full
 * Don't delete diff backup if:
   - we have less than 3 diffs
   - we have any of incremental backup dependent on this diff
 * Don't delete incremental backup if:
   - we have any of incremental backup dependent on this incremental

# Bareos prerequisite config
 * Make sure that `Recycle = No` is set in bacula configs for all volumes
   - if you have any vols with Recycle = yes script tell you about it
 * Script now support setup with one device with same relative archive path 

# Install
`sudo crontab -e -u bareos`  
`3 5 * * * /path/to/delete_purged_volumes_bareos.py`  
Run (at 5:03) any time before/after all backup done  

# Config
Change this vars in top of script  
```
dry_run
my_catalog_name
my_sd_device_name
sd_conf, storages_conf, dir_conf
```

