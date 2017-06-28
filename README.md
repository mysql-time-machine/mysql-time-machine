# MySQL Time Machine

MySQL Time Machine is a collection of services and tools for creating, processing and storing streams of MySQL data changes. Its main components are presented bellow.

# Repositories
The MySQL Time Machine project is divided across several GitHub repositories.

- [Binlog flusher](https://github.com/mysql-time-machine/replicator/tree/master/binlog-flusher)
- [Replicator](https://github.com/mysql-time-machine/replicator)
- [HBase snapshotter](https://github.com/mysql-time-machine/hbase-snapshotter)


# Components:
## [Binlog Flusher](https://mysql-time-machine.github.io/replicator/#1_binlog_flusher)
Flushes MySQL database tables to the binlog in order to have the initial snapshot of the database in the binlog.

## [Replicator](https://mysql-time-machine.github.io/#replicator)
Replicates data changes from MySQL binlog to HBase, preserving the previous data versions. Intended
for auditing purposes of historical data. In addition can maintain special daily-changes tables which
are convenient for fast and cheap imports from HBase to Hive.

## [HBase Snapshotter](https://mysql-time-machine.github.io/snapshotter/)
HBaseSnapshotter is a Spark application that takes a snapshot of an HBase table at a given point in time and stores it to a Hive table. Usually you can export from HBase to Hive but you can only get the latest version, as Hive doesn't have enough flexibility to access different versions of an HBase table. Spark framework allows this flexibility since it has the ability and the API to access and manipulate both HBase and Hive.

# AUTHOR
Bosko Devetak <bosko.devetak@gmail.com>

# CONTRIBUTORS

- Daniël van Eeden <a href="https://github.com/dveeden">dveeden</a>
- Greg Franklin <a href="https://github.com/gregf1">gregf1</a>
- Islam Hassan <a href="https://github.com/ishassan">ishassan</a>
- Mikhail Dutikov <a href="https://github.com/mikhaildutikov">mikhaildutikov</a>
- Pavel Salimov <a href="https://github.com/chcat">chcat</a>
- Pedro Silva <a href="https://github.com/pedros">pedros</a>
- Rares Mirica <a href="https://github.com/mrares">mrares</a>
- Raynald Chung <a href="https://github.com/raynald">raynald</a>

# ACKNOWLEDGMENT
MySQL Time Machine was originally developed for Booking.com. With approval from Booking.com, the code and specification were generalized and published as Open Source on github, for which the author would like to express his gratitude.

# COPYRIGHT AND LICENSE
Copyright (C) 2015, 2016, 2017 by Author and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
