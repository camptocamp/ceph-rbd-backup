# ceph-rbd-backup
Ceph RBD images replication / backup tool

This tool allows managing daily snapshots of RBD images on a Ceph cluster
and replicating these snapshots to a backup cluster.


## Invocation

```
Usage: ceph-rbd-backup.py ACTION [-h | --help] [--image IMAGE] [--debug]
```

`-h` or `--help` displays a standard _usage_ screen.

If `--image IMAGE` is specified, only this particular image is used, istead
of all the images found in the production Cluster or mounted on a server. This
affects all three actions `snapshot`, `replicate`, and `check`.

`--debug` logs additional information, including all `rbd` command invocations
and their output.


## Actions

`ceph-rbd-backup` can perform several actions based on invocation.

### `snapshot`

`ceph-rbd-backup` is designed to create snapshots of RBD images from the
servers / instances where they are mapped and mounted. It takes care of
freezing the filesystem before creating the snapshot. It is meant to be run
automatically every day using `cron`. It creates a snapshot in the format
`YYYY-MM-DD`.

### `replicate`

This tool manages replication of the RBD image's snapshots from a production
Ceph cluster to a backup cluster. This is meant to be run from a central
server that has access to both Ceph clusters, and must be run after all daily
snapshots have been created.

### `check`

The `check` action verifies snapshots on the production cluster and
replication on the backup cluster. It's output conforms to the Nagios plugin
API and so can be called directly from a Nagios service check.


## Credits

This script is inpired by this Bash script published by Rapide Internet:

https://www.rapide.nl/blog/item/ceph_-_rbd_replication.html


## License

This script is publish under the GNU GPL v2 license (see the `LICENSE` file).
