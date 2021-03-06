====================
 AppScale Datastore
====================

A server that implements the `Google Cloud Datastore`_ API. At this time, it
only supports v3 protocol buffer requests generated by the SDK runtimes.

One example of using this as a standalone service is redirecting datastore
requests from an application running in GAE to some other environment with
this service running.

How to set up
=============

1. `Start a Cassandra cluster`_

   * You can use cassandra.yaml and cassandra-env.sh in
     appscale/datastore/cassandra-env/templates as a starting point if you
     replace all options that start with "APPSCALE-"
   * Make sure you use the ByteOrderedPartitioner
   * Specifying num_tokens as 1 for all nodes is recommended if you want to be
     able to rebalance your cluster

2. `Start a ZooKeeper cluster`_
3. Install the appscale-datastore Python package with ``pip install ./AppDB``
4. Create the following files (the file names are not applicable to Cassandra,
   but they are an artifact from when AppScale used different databases):

   * /etc/appscale/masters: This should contain the private IP address of any
     one of the Cassandra machines.
   * /etc/appscale/slaves: This should contain the IP addresses of the
     remaining machines in the cluster. Each address should be on a different
     line.

5. Create a file called /etc/appscale/zookeeper_locations with the content
   ::

      zk-ip-1
      zk-ip-2

   (where zk-ip-1 and zk-ip-2 are the locations of the machines
   in the ZooKeeper cluster).
6. Run ``appscale-prime-cassandra --replication x`` where "x" is the
   replication factor you want for the datastore's keyspace.
7. Start the datastore server with ``appscale-datastore -p x`` where "x" is the
   port you would like to start the server on. You probably want to start more
   than one since each can currently only handle one request at a time.
   AppScale starts 2x the number of cores on the machine.

.. _Google Cloud Datastore: https://cloud.google.com/datastore/
.. _Start a Cassandra cluster:
   http://cassandra.apache.org/doc/latest/getting_started/index.html
.. _Start a ZooKeeper cluster:
   https://zookeeper.apache.org/doc/trunk/zookeeperStarted.html
