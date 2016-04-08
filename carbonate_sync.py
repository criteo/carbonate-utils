#!/usr/bin/env python

from __future__ import print_function

import collections
import errno
import getpass
import inspect
import itertools
import logging
import multiprocessing
import os
import progressbar
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from carbon import util
import carbonate.cli as carbonate_cli
import carbonate.config as carbonate_config
import carbonate.cluster as carbonate_cluster
import carbonate.sieve as carbonate_sieve
import carbonate.sync as carbonate_sync
import carbonate.util as carbonate_util

_DEFAULT_TMP_DIR = tempfile.gettempdir()
_MAX_PARALLEL_RSYNC = 4

HAVE_HEAL_WITH_TIME_RANGE = (
    set(['start_time', 'end_time']) <=
    set(inspect.getargspec(carbonate_sync.heal_metric).args))

STORAGE_DIR = carbonate_cli.STORAGE_DIR

MANDATORY_SSH_OPTIONS = [
    '-o', 'PasswordAuthentication=no',
    '-o', 'LogLevel=quiet',  # Removes warnings
]

SSH_OPTIONS = [
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o', 'Compression=no',
]

RSYNC_OPTIONS = [
    '--archive', '--sparse',
    '--update',
    # Keep files from a previous unfinished run, unless they are older than
    # a day
    '--modify-window=100800',
]

DEFAULT_EXCLUDE = [
    '^carbon\.'
]

# Set to False to use the slow and serialized version. For debug
# purposes.
PARALLEL = True


class Error(Exception):
    """Base class for exceptions from this module."""


def _abort(*args, **kwargs):
    """Print error message and exit."""
    kwargs.setdefault('file', sys.stderr)
    print(*args, **kwargs)
    sys.exit(1)


def _run(cmd, *args, **kwargs):
    """Starts a process and return its output."""
    kwargs.setdefault('stdout', subprocess.PIPE)
    kwargs.setdefault('stderr', subprocess.STDOUT)
    popen = subprocess.Popen(cmd, *args, **kwargs)
    stdout, stderr = popen.communicate()
    if popen.wait():
        description = ' '.join(cmd) if isinstance(cmd, list) else cmd
        output = stderr or stdout or '<No output>'
        raise Error('Subcommand failed: %s: %s' % (description, output))
    if stdout:
        return [l for l in stdout.split('\n') if l]


def _ssh(host, cmd, ssh_options=SSH_OPTIONS):
    """ssh to an host and run a command."""
    ssh_cmd = ['ssh'] + MANDATORY_SSH_OPTIONS + ssh_options + [host, '--'] + cmd
    return _run(ssh_cmd)


def _filter_metrics(metrics, exclude):
    """Exclude metrics matching a list of regexp."""
    for pattern in exclude:
        regex = re.compile(pattern)
        metrics = [m for m in metrics if not regex.search(m)]
    return metrics


def _list_metrics(config, local_cluster, local_node, remote_node, exclude,
                  ssh_options=[]):
    """List metrics on a remote node."""

    metrics = _ssh(remote_node, ['carbon-list'], ssh_options=ssh_options)
    metrics = _filter_metrics(metrics, exclude)

    cluster = carbonate_cluster.Cluster(config, local_cluster)
    metrics = carbonate_sieve.filterMetrics(
        metrics, local_node, cluster, invert=False)
    return sorted(metrics)


def info(msg=''):
    logging.info(msg)


def _fetch_from_remote(batch):
    """Fetch a remote list of files using rsync."""
    remote = '%s@%s:%s/' % (batch.remote_user, batch.remote_node, STORAGE_DIR)
    with tempfile.NamedTemporaryFile('w', delete=False) as f:
        for m in batch.metrics_fs:
            print(m, file=f)
        f.flush()
        ssh_cmd = 'ssh ' + ' '.join(MANDATORY_SSH_OPTIONS + batch.ssh_options)
        cmd = [
            'rsync',
            '--rsh', ssh_cmd,
            '--files-from=%s' % f.name,
        ]
        cmd += batch.rsync_options
        cmd += [
            remote,
            batch.staging_dir,
        ]
        _run(cmd)

    return batch


def _make_staging_dir(base_dir, node):
    """Creates the staging directory for this node."""
    dirname = os.path.join(base_dir, node)
    try:
        os.makedirs(dirname)
    except OSError as exc:
        # Ignore already existing directories
        if exc.errno != errno.EEXIST or not os.path.isdir(dirname):
            raise
    return dirname


def _heal(batch):
    """Heal whisper files.

    This method will backfill data present in files in the
    staging dir if not present in the local files for
    points between 'start' and 'stop' (unix timestamps).
    """

    for metric in batch.metrics_fs:
        src = os.path.join(batch.staging_dir, metric)
        dst = os.path.join(STORAGE_DIR, metric)
        if HAVE_HEAL_WITH_TIME_RANGE:
            carbonate_sync.heal_metric(
                src, dst, start_time=batch.start_time, end_time=batch.end_time)
        else:
            carbonate_sync.heal_metric(src, dst)

    return batch

class _Batch(collections.namedtuple('_Batch',
    ['staging_dir', 'metrics_fs',
     'start_time', 'end_time',
     'remote_user', 'remote_node',
     'rsync_options', 'ssh_options'])):

    def split_chunks(self, chunksize):
        res = []
        for n in range(0, len(self.metrics_fs), chunksize):
            new_batch = self._replace(metrics_fs=self.metrics_fs[n:n+chunksize])
            res.append(new_batch)
        return res

    @property
    def metrics_number(self):
        return len(self.metrics_fs)


def _fetch_merge(fetch_batches, update_cb):
    """fetch & merge files in the staging directory with local files."""
    total_metrics_number = sum(b.metrics_number for b in fetch_batches)

    # Estimates how many percent of total time is fetching data
    fetch_percent = 10
    heal_percent = 100 - fetch_percent

    work_done = 0
    update_cb(work_done)

    if not PARALLEL:
        for batch in fetch_batches:
            _fetch_from_remote(batch)
            _heal(batch)
            work_done += batch.metrics_number * (fetch_percent + heal_percent)
            update_cb(work_done // total_metrics_number)
    else:
        rsync_workers = multiprocessing.Pool(_MAX_PARALLEL_RSYNC)
        heal_workers = multiprocessing.Pool()

        for batch_fetched in rsync_workers.imap_unordered(_fetch_from_remote, fetch_batches):
            # Updates estimate of work done
            work_done += fetch_percent * batch_fetched.metrics_number
            update_cb(work_done // total_metrics_number)

            # Subdivides heal batches to ensure we use all CPUs.
            # Smaller batches increases serialisation overhead.
            heal_chunksize = batch_fetched.metrics_number / multiprocessing.cpu_count()
            heal_batches = batch_fetched.split_chunks(heal_chunksize)
            # This waits for all chunks of a fetch to heal before healing the next fetch.
            # This is OK as heal chunks are all about the same size and will complete at once.
            for batch_healed in heal_workers.imap_unordered(_heal, heal_batches):
                work_done += heal_percent * batch_healed.metrics_number
                update_cb(work_done // total_metrics_number)

        for workers in rsync_workers, heal_workers:
            workers.close()
            workers.join()


def _get_nodes(config, cluster_name, node=None):
    """Get the list of nodes in another cluster, excludes 'node'."""
    res = set()
    for entry in util.parseDestinations(config.destinations(cluster_name)):
        res.add(entry[0])  # Only keep the host.
    if node in res:
        res.remove(node)
    return res


def _parse_args():
    """Parse command line arguments."""
    parser = carbonate_util.common_parser(description='carbonate-sync')
    parser.add_argument(
        '-S', '--remote-cluster',
        default=None,
        help='Remote cluster name')

    parser.add_argument(
        '-n', '--node',
        default=socket.gethostname(),
        help='Name of the local node (same as in carbonate.conf).')

    parser.add_argument(
        '-b', '--batch-size',
        default=1000,
        help='Batch size for fetching metrics.')

    parser.add_argument(
        '--ssh-options',
        default=' '.join(SSH_OPTIONS),
        help='Pass option(s) to ssh. Make sure to use ' +
        '"--ssh-options=" if option starts with \'-\'')

    parser.add_argument(
        '--rsync-options',
        default=' '.join(RSYNC_OPTIONS),
        help='Pass option(s) to rsync. Make sure to use ' +
        '"--rsync-options=" if option starts with \'-\'')

    parser.add_argument(
        '--temp-dir',
        default=_DEFAULT_TMP_DIR,
        help='Temporary dir')

    parser.add_argument(
        '--exclude',
        default=','.join(DEFAULT_EXCLUDE),
        help='Comma separated regexp of paths to exclude (slow).')

    parser.add_argument(
        '--start-time', type=int,
        default=0,
        help='Sync data more recent than this time.')

    parser.add_argument(
        '--end-time', type=int,
        default=time.time(),
        help='Sync data older than this time (default: now).')

    args = parser.parse_args()
    if args.remote_cluster is None:
        args.remote_cluster = args.cluster

    # We want lists for these options.
    args.rsync_options = shlex.split(args.rsync_options)
    args.ssh_options = shlex.split(args.ssh_options)
    args.exclude = args.exclude.split(',')
    return args


def main():
    log_format = (
        '%(asctime)s.%(msecs)d %(levelname)s %(module)s - '
        '%(funcName)s: %(message)s')
    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S")

    args = _parse_args()
    config = carbonate_config.Config(args.config_file)

    local_cluster = args.cluster
    local_user = config.ssh_user(local_cluster)
    local_node = args.node
    remote_cluster = args.remote_cluster
    remote_nodes = _get_nodes(config, remote_cluster, node=local_node)
    base_dir = args.temp_dir

    if getpass.getuser() != local_user:
        _abort('This program must be run as the "%s" user' % local_user)

    info('- I am %s part of cluster %s and trying to sync with %s' %
         (local_node, local_cluster, remote_cluster))

    for n, node in enumerate(remote_nodes):
        info('- Syncing node %s (%d/%d)' % (node, n+1, len(remote_nodes)))
        info('- %s: Listing metrics' % node)

        metrics = _list_metrics(
            config, local_cluster, local_node, node, args.exclude,
            ssh_options=args.ssh_options)
        staging_dir = _make_staging_dir(base_dir, node)
        metrics_fs = [carbonate_util.metric_to_fs(m) for m in metrics]

        batches = _Batch(
            staging_dir=staging_dir,
            metrics_fs=metrics_fs,
            start_time=args.start_time,
            end_time=args.end_time,
            remote_user=config.ssh_user(remote_cluster),
            remote_node=node,
            rsync_options=args.rsync_options,
            ssh_options=args.ssh_options,
        ).split_chunks(args.batch_size)

        info('- %s: Merging and fetching %s metrics' % (node, len(metrics)))
        with progressbar.ProgressBar(redirect_stdout=True) as bar:
            _fetch_merge(batches, bar.update)

        info('  %s: Cleaning up' % node)
        shutil.rmtree(staging_dir)

        info('  %s: Done' % node)

        info('')


if __name__ == '__main__':
    main()
