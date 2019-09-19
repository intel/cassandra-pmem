# Getting started with the Persistent Memory storage engine fork of Cassandra

## PREREQUISITES TO BUILD
Build and setup the Low Level Persistence Library (LLPL) available at [LLPL](http://github.com/pmem/llpl). <br> 
After building LLPL, 
* Copy ```llpl.jar``` to Cassandra's ```lib``` directory 
* Copy ```libllpl.so``` to ```lib/sigar-bin```<br><br>

Build Cassandra as usual.

## CONFIGURATION NOTES
Follow the instructions for setting up persistent memory as described in the LLPL Readme. Specify the path and size of the persistent memory pool by setting the following properties in ```conf/jvm.options```.
* ```-Dpmem_path=```<_/path/pool_file_name_>
* ```-Dpool_size=```<_pool_size_in_bytes_>
