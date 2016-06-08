# Carbonate Utils

Utilities to plumb carbonate tools together.
Depends on the master (and yet unreleased) branches of whisper, carbonate and carbon.

## The Tools

### carbonate-sync

```bash
$ carbonate-sync --help
usage: carbonate-sync [-h] [-c CONFIG_FILE] [-C CLUSTER] [-S REMOTE_CLUSTER]
                      [-n NODE] [-b BATCH_SIZE] [--ssh-options SSH_OPTIONS]
                      [--rsync-options RSYNC_OPTIONS] [--temp-dir TEMP_DIR]
                      [--exclude EXCLUDE] [--start-time START_TIME]
                      [--end-time END_TIME] [--overwrite OVERWRITE]

carbonate-sync

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        Config file to use (default:
                        /opt/graphite/conf/carbonate.conf)
  -C CLUSTER, --cluster CLUSTER
                        Cluster name (default: main)
  -S REMOTE_CLUSTER, --remote-cluster REMOTE_CLUSTER
                        Remote cluster name (default: None)
  -n NODE, --node NODE  Name of the local node (same as in carbonate.conf).
                        (default: coulomier)
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        Batch size for fetching metrics. (default: 1000)
  --ssh-options SSH_OPTIONS
                        Pass option(s) to ssh. Make sure to use "--ssh-
                        options=" if option starts with '-' (default: -o
                        StrictHostKeyChecking=no -o
                        UserKnownHostsFile=/dev/null -o Compression=no)
  --rsync-options RSYNC_OPTIONS
                        Pass option(s) to rsync. Make sure to use "--rsync-
                        options=" if option starts with '-' (default:
                        --archive --sparse --update --modify-window=100800)
  --temp-dir TEMP_DIR   Temporary dir (default: /tmp)
  --exclude EXCLUDE     Comma separated regexp of paths to exclude (slow).
                        (default: ^carbon\.)
  --start-time START_TIME
                        Sync data more recent than this time. (default: 0)
  --end-time END_TIME   Sync data older than this time (default: now).
                        (default: 1465420874.04)
  --overwrite OVERWRITE
                        Overwrite local data with remote data (default:
                        false). (default: False)
```

# Authors

 * Brice Arnould <b.arnould@criteo.com>
 * Corentin Chary <c.chary@criteo.com>

# License and warnings

These tools should be considered beta quality right now.
Tests exist for most functionality, but there is still significant work to be done to make them bullet-proof.

The code is available under the MIT license.
