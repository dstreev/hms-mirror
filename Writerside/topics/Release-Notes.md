# Release Notes

## v.2.x Changes

### Concurrency

In previous releases using the CLI, concurrency could be set through the configuration files `transfer:concurrency` setting.  The default was 4 if no setting was provided.  This setting in the control file is NO LONGER supported and will be ignored.  The new default concurrency setting is `10` and can be overridden only during the application startup.

See [Concurrency](Concurrency.md) for more details.

### Global Location Maps

Previous releases had a fairly basic implementation of 'Global Location Maps'.  These could be supplied through the 
cli option `-glm`, which is still supported, but limited in functionality. The improved implementation work from the 
concept of building 'Warehouse Plans' which are then used to build the 'Global Location Maps'. 

See [Warehouse Plans]() for more details.

### JDK 11 Support

The application now supports JDK 11, as well as JDK 8.

### Kerberos Support

We are still working to replicate the options available in previous release with regard to Kerberos connections.  Currently, `hms-mirror` can only support a single Kerberos connection.  This is the same as it was previously.  `hms-mirror` packaging includes the core Hadoop classes required for Kerberos connections pulled from the latest CDP release.

In the past, we 'could' support kerberos connections to lower versions of Hadoop clusters (HDP and CDH) by running `hms-mirror` on a cluster with those hadoop libraries installed and specifying `--hadoop-classpath` on the commandline. This is no longer supported, as the packaging required to support the Web and REST interfaces is now different.

We are investigating the possibility of supporting kerberos connections to lower clusters in the future.


