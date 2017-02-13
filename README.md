## Pipeline

A subscription based distributed message processing system.

### Concepts
#### Roles
Say you have a real time message processing system, each component is in charge of a specific task or job.
And it could be playing the role of:

##### Hydrant
The entrance where the data got input to your system.
It grabs data from MQTT, Kafka, Logs... and throw it to the system.

##### Pipe
Where the data got processed.
You would have multiple Pipes, chained, forked, joined... like a workflow.

##### Outlet
Where the data flows out of the system, saving to logs, database...

#### Topology
There's not centralized configuration for your message processing system's topology, it's based on subscription.

Each component is an Actor in your system, playing a different role.
Each Actor has a role, and if it's a `Pipe` or `Outlet`, it should also have *interested actors*.
This forms up a subscribing topology, where messages will flow through the `Pipeline`, maybe from `Hydrant` to `Outlet`,
 or one `Pipe` to another `Pipe`.

So that you can dynamically add *actors* into the deployed running system.
