This is a simple utility to migrate Kafka consumer group IDs by cloning the offsets of an existing group to a new group for a given topic.

Usage:

````
 migrate-consuler-group <action> <broker> <topic> <existing.group.id> <new.group.id> [<login> <password>]
  action is one of:
    try        do not proceed with any change
    do         proceed but abort if new the group already exists
    do-force   proceed even if new the group already exists```
````

How to build:

```
go build .
```
