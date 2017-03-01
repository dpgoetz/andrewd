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

Config file explanation

    [DEFAULT]
    user = swift
    
    [andrewd]
    working_dir = /some/dir/no/default  # where the sqlite db is kept
    report_dir = /some/dir/no/default  # where the json report is saved
    run_frequency = 3600  # how often (in secs) this runs and produces report
    ring_update_frequency = 259200  # how often it will update / rebalance the ring
    max_age_sec = 604800  # if a drive has been unmounted / unreachable 
                          # for this long. it will be set to 0 weight
    max_weight_change = .005  # on a ring update. this is the max ratio of device 
                              # weight that will be allowed to be removed
    ring_builder = /etc/swift/object.builder  # the ring builder. the deployed
	                                          # object.ring.gz must be in
                                              # /etc/swift/object.ring.gz
    log_facility = LOG_LOCAL0  # logs over udp
    do_not_rebalance = false  # if set to true will run set_weight commands 
                              # but will not rebalance the ring
    

TODO:
- Create a webserver to show report and then later to be able to make rings changes.
- Make priority replication calls after the ring change to heal the cluster.
