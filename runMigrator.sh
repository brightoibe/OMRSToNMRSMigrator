#!/bin/bash 


export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
export PATH=$JAVA_HOME/bin:$PATH
export JDK_HOME=$JAVA_HOME
java -jar OMRSToNMRSMigrator.jar

