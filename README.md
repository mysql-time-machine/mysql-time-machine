# MySQL Time Machine
Collection of services and tools for creating, processing and storing streams of MySQL data changes.

# Status
Testing, beta-level quality.

# Components:

## Binlog Flusher
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

### Usage
Flush database to binlog with:
````
python data-flusher.py --mycnf .my.cnf --host $host [--db $db] [--table $table]
````
Where .my.cnf contains the admin privileges used for the blackhole_copy of initial snapshot.
````
[client]
user=admin
password=admin
````
Then start replication with
````
mysql> start slave;
````

## MySQL to HBase Replicator.
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data. In addition can maintain special daily-changes tables which
are convenient for fast and cheap imports from HBase to Hive.

### Usage
#### Replicate initial binlog snapshot to hbase
````
java -jar hbrepl-0.9.9-3.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $first-binlog-filename --config-path $config-path [--shard $shard] --initial-snapshot
````

#### Replication after initial snapshot
````
java -jar hbrepl-0.9.9-3.jar --hbase-namespace $hbase-namespace --applier hbase --schema $schema --binlog-filename $binlog-filename --config-path $config-path [--shard $shard] [--delta]
````

#### Replicate range of binlog files and output db events as JSON to STDOUT:
````
java -jar hbrepl-0.9.9-3.jar --applier STDOUT --schema $schema --binlog-filename $binlog-filename --last-binlog-filename $last-binlog-filename-to-process --config-path $config-path 
````

#### Configuration file structure:
````
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
hbase:
    namespace: 'schema_namespace'
    zookeeper_quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
graphite:
    url:       'graphite_host[:<graphite_port>]'
    namespace: 'no-stats'
hive_imports:
    tables: ['sometable']
````

# AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

# CONTRIBUTORS
Greg Franklin <a href="https://github.com/gregf1">gregf1</a>

Rares Mirica <a href="https://github.com/mrares">mrares</a>

Mikhail Dutikov <a href="https://github.com/mikhaildutikov">mikhaildutikov</a>


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

