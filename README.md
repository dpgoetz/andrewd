AndrewD
=======

This OpsRobot is a daemon that will help run your swift cluster.
Currently it keeps track of unmounted drives in a swift cluster
and if they go unmounted for more than 3 days (by default) it
will remove the drive from the ring.

It uses the recon api on the swift object server to query the unmounted drives
on the cluster so it requires network access to your storage nodes to run.

The unmounted drives will be kept track of using a local sqlite db.

It creates a json report of its progress in a configurable location.

TODO:
- Create a webserver to show report and then later to be able to make rings changes.
- Make priority replication calls after the ring change to heal the cluster.
