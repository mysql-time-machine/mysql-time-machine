# MySQL to HBase Replicator.
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data.

# STATUS
In testing, beta-level quality.

# USAGE

## Initial snapshot

### Flush db to binlog
python data-flusher.py --mycnf .my.cnf --host $host [--db $db] [--table $table]

### replicate initial snapshot to hbase
java -jar hbrepl-0.9.9-1.jar --hbase-namespace $hbase-namespace --applier $applier --schema $schema --binlog-filename $first-binlog-filename --config-path $config-path --shard $shard --initial-snapshot

### Replication
java -jar hbrepl-0.9.9.jar --hbase-namespace $hbase-namespace --applier $applier --schema $schema --binlog-filename $binlog-filename --config-path $config-path --shard $shard --delta

# CONFIGURATION
One yml file for replicator. Example of config file:

    replication_schema:
        name:     'replicated_schema_name'
        username: 'user'
        password: 'pass'
        slaves:   ['localhost', 'localhost']
    metadata_store:
        username: 'user'
        password: 'pass'
        host:     'active_schema_host'
        database: 'active_schema_database'
    zookeepers:
        quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
    graphite:
        url:       'graphite_host[:<graphite_port>]'
        namespace: 'no-stats'
    hive_imports:
        replicated_schema_name: ['sometable']

One .my.cnf file containing admin privileges used for the blackhole_copy initial snapshot.
````
    [client]
    user=admin
    password=admin
````

# AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

# ACKNOWLEDGMENT
Replicator was originally developed for Booking.com. With approval from Booking.com, the code and specification were generalized and published as Open Source on github, for which the author would like to express his gratitude.

# COPYRIGHT AND LICENSE
Copyright (C) 2015 by Bosko Devetak

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

