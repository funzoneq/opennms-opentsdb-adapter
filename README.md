opennms-opentsdb-adapter
========================

Prototype for an adapter to push OpenNMS performance data into OpenTSDB

To build, run "mvn assembly:assembly".

This is a client receiver for the TCP RRD strategy.  It receives RRD updates
over IP and prints them to STDOUT.

Usage: java -jar perfdata-receiver-X.X.jar [port]
