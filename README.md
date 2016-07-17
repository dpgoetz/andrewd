AndrewD
=======

This OpsRobot is a daemon that will help run your swift cluster.
Currently it keeps track of unmounted drives in a swift cluster
and if they go unmounted for more than a week (by default) it
will remove the drive from the ring. It will then use
`hummingbird moveparts` to get the cluster rebalanced.

It uses the recon api on the swift object server to query the unmounted drives
on the cluster.

The unmounted drives will be kept track of using the swift cluster itself. You
just need to supply auth credentials to an account. 

This will also set up a staticweb site displaying cluster status, etc.
