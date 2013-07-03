hfaa
====

Hadoop FileSystem Agnostic API - extends Hadoop's abstract file system api to a generic TCP sockets interface. File system operations like read, write, delete, rename, etc. are made available to an API server that follows the HFAA op codes. In a nutshell, this abstracts Hadoop's MapReduce interface one step further to move file system integration to the file system itself. This removes the requirement to tinker with Hadoop internals and allows any file system with TCP socket support to integrate with Hadoop MapReduce.
