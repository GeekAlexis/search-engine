# Crawler

Yuchen Ding (ycding@seas.upenn.edu)

The crawler section is designed for CIS 555 final project, and it is based on previous homework in class. It has three
components: crawler, storage, and stormlite.

The first component is the driver program for setting up configurations and building up stormlite topology, and it
also includes helper classes and functions for actual crawling implementation; the storage component is responsible
for data transferring between storage; and the stormlite package includes the core implementation of this crawler.

This subproject is built by Maven with two execuations, crawler and download. The first one is for crawling, and
the second one is only for data transferring purpose.

All specifications are satisfied, and all features are implemented.

Source files: StormLite and MapReduce from HW, postgresql JDBC, AWS JDK

Instruction within crawler folder:
mvn clean install
mvn exec:java@crawler

