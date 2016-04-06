#!/usr/bin/env python

from __future__ import print_function

import errno
import getpass
import itertools
import logging
import multiprocessing
import os
import progressbar
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import shlex
import re

from carbon import util
import carbonate.cli as carbonate_cli
import carbonate.config as carbonate_config
import carbonate.cluster as carbonate_cluster
import carbonate.sieve as carbonate_sieve
import carbonate.sync as carbonate_sync
import carbonate.util as carbonate_util

_DEFAULT_TMP_DIR = tempfile.gettempdir()
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
        metrics = [m for m in metrics if not regex.match(m)]
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


def _fetch_from_remote(remote_user, remote_host, metrics_fs, staging_dir,
                       rsync_options=RSYNC_OPTIONS, ssh_options=SSH_OPTIONS):
    """Fetch a remote list of files using rsync."""
    remote = '%s@%s:%s/' % (remote_user, remote_host, STORAGE_DIR)
    with tempfile.NamedTemporaryFile('w', delete=False) as f:
        for m in metrics_fs:
            print(m, file=f)
        f.flush()
        ssh_cmd = 'ssh ' + ' '.join(MANDATORY_SSH_OPTIONS + ssh_options)
        cmd = [
            'rsync',
            '--rsh', ssh_cmd,
            '--files-from=%s' % f.name,
        ]
        cmd += rsync_options
        cmd += [
            remote,
            staging_dir,
        ]
        _run(cmd)


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


def _heal(args_and_metric):
    """Heal whisper files.

    This method will backfill data present in files in the
    staging dir if not present in the local files for
    points between 'start' and 'stop' (unix timestamps).
    """
    args, metric = args_and_metric
    staging_dir, start, end = args
    carbonate_sync.heal_metric(
        os.path.join(staging_dir, metric),
        os.path.join(STORAGE_DIR, metric),
        # TODO(c.chary): implement that in carbonate
        # start=start, end=end
    )


def _merge_from_staging(staging_dir, metrics_fs, update_cb, chunksize,
                        start, end):
    """Merge files in the staging directory with local files."""

    # An iterable of pairs of ((staging_dir, start, end), metric)
    args = staging_dir, start, end
    args_and_metrics = itertools.izip(itertools.repeat(args), metrics_fs)

    done = 0
    update_cb(done)

    if PARALLEL:
        workers = multiprocessing.Pool()
        for _ in workers.imap_unordered(
                _heal, args_and_metrics, chunksize=chunksize):
            done += 1
            update_cb(done)
        workers.close()
        workers.join()
    else:
        for i in args_and_metrics:
            _heal(i)


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
        help='Batch size for the merge job.')

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
    remote_user = config.ssh_user(remote_cluster)
    remote_nodes = _get_nodes(config, remote_cluster, node=local_node)
    base_dir = args.temp_dir

    if getpass.getuser() != local_user:
        _abort('This program must be run as the "%s" user' % local_user)

    info('- I am %s part of cluster %s and trying to sync with %s' %
         (local_node, local_cluster, remote_cluster))

    for n, node in enumerate(remote_nodes):
        info('- Syncing node %s (%d/%d)' % (node, n, len(remote_nodes)))
        info('- %s: Listing metrics' % node)

        metrics = _list_metrics(
            config, local_cluster, local_node, node, args.exclude,
            ssh_options=args.ssh_options)
        staging_dir = _make_staging_dir(base_dir, node)
        metrics_fs = [carbonate_util.metric_to_fs(m) for m in metrics]

        info('- %s: Fetching %d metrics' % (node, len(metrics)))
        _fetch_from_remote(remote_user, node, metrics_fs, staging_dir,
                           rsync_options=args.rsync_options,
                           ssh_options=args.ssh_options)

        info('- %s: Merging %s metrics' % (node, len(metrics)))
        with progressbar.ProgressBar(
                max_value=len(metrics), redirect_stdout=True) as bar:
            _merge_from_staging(staging_dir, metrics_fs, bar.update,
                                chunksize=args.batch_size,
                                start=args.start_time,
                                end=args.end_time)

        info('  %s: Cleaning up' % node)
        shutil.rmtree(staging_dir)

        info('  %s: Done' % node)

        info('')


if __name__ == '__main__':
    main()
