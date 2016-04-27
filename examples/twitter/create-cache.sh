#!/usr/bin/env bash

function create-cache()
{
   docker exec -ti $1 /opt/jboss/infinispan-server/bin/ispn-cli.sh -c command="/subsystem=datagrid-infinispan/cache-container=clustered/configurations=CONFIGURATIONS/distributed-cache-configuration=$2:add(start=EAGER,mode=SYNC,template=false)"
   docker exec -ti $1 /opt/jboss/infinispan-server/bin/ispn-cli.sh -c command="/subsystem=datagrid-infinispan/cache-container=clustered/distributed-cache=$2:add(configuration=$2)"
}

create-cache "ispn-1"  "exampleCache"
create-cache "ispn-2"  "exampleCache"