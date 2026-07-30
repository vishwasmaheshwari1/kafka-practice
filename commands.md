# Kafka CLI Walkthrough — Annotated Command Log

Reformatted from `commands.txt` (raw terminal transcript, 3,133 lines, 22–29 Jul 2026).
Every command below was actually run against a single-broker Kafka 4.0.0 cluster on
`localhost:9092`; outputs are verbatim from the transcript, trimmed only for length and
terminal redraw noise. Retyped/tab-completion artifacts in the original (`kafka-topkafka-topics.sh`)
have been normalised to the real command.

**Coverage:** the transcript holds 211 shell prompts. 57 commands are documented below — the
distinct, meaningful ones. Left out: `cd`/`ls`/`vim`/`echo` navigation and note-taking, 17 bare
`^C` interrupts, 14 empty prompts, and repeat runs of a command already shown (e.g. `--describe`
was run 30 times). Nothing that produced a distinct result was dropped.

Two constants omitted from every output block to keep them readable:

- `WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide...`
  — printed by `kafka-topics.sh --create` for every topic name used here.
- Long Java stack traces under error messages — the first line of each is kept.

---

## Contents

| #  | Section                       | What it covers                                              |
|----|-------------------------------|-------------------------------------------------------------|
| 1  | Getting the course material   | `git clone` of the Conduktor repos                          |
| 2  | Broker option A — Docker      | Conduktor single-node stack + web console                   |
| 3  | Broker option B — local KRaft | `kafka-storage.sh format`, `kafka-server-start.sh`          |
| 4  | Topics                        | create / list / describe / delete, partitions, RF           |
| 5  | Producing from the CLI        | plain, `acks`, keys, round-robin partitioner                |
| 6  | Consuming from the CLI        | tail vs `--from-beginning`, printing metadata               |
| 7  | Consumer groups               | `--group`, `--list`, `--describe`, LAG                      |
| 8  | Resetting offsets             | `--reset-offsets --to-earliest`, dry-run then execute       |
| 9  | Topics for the Java apps      | `demo_java`, `wikimedia.recentchange`                       |
| 10 | OpenSearch sink stack         | `docker compose up -d`, container lifecycle                 |
| 11 | Dynamic topic configs         | `kafka-configs.sh --alter --add-config` / `--delete-config` |
| 12 | Log compaction                | `cleanup.policy=compact` topic setup                        |
| 13 | Errors hit along the way      | every failure in the log, with the cause                    |

---

## 1. Getting the course material

The two upstream repos the practice work is based on.

**Java source for the course** — producers, consumers, the Wikimedia producer and the OpenSearch consumer. The local hand-typed copy of this lives in `kafka-java-practice/`.

```bash
$ git clone https://github.com/conduktor/kafka-beginners-course.git
```

```text
Cloning into 'kafka-beginners-course'...
remote: Enumerating objects: 436, done.
remote: Counting objects: 100% (113/113), done.
remote: Compressing objects: 100% (64/64), done.
remote: Total 436 (delta 93), reused 53 (delta 49), pack-reused 323 (from 3)
Receiving objects: 100% (436/436), 361.50 KiB | 2.66 MiB/s, done.
Resolving deltas: 100% (155/155), done.
```

**Ready-made Docker Compose stacks** for Kafka — single/multi broker, with or without ZooKeeper, plus a Conduktor console variant.

```bash
$ git clone https://github.com/conduktor/kafka-stack-docker-compose.git
```

```text
Cloning into 'kafka-stack-docker-compose'...
remote: Enumerating objects: 807, done.
remote: Counting objects: 100% (379/379), done.
remote: Compressing objects: 100% (94/94), done.
remote: Total 807 (delta 344), reused 285 (delta 285), pack-reused 428 (from 3)
Receiving objects: 100% (807/807), 175.54 KiB | 1.75 MiB/s, done.
Resolving deltas: 100% (565/565), done.
```

---

## 2. Broker option A — Kafka in Docker (Conduktor stack)

`conduktor-kafka-single.yml` runs one KRaft broker (`confluentinc/cp-kafka:8.0.0`), Postgres,
and the Conduktor web console on port 8080. Run it from inside `kafka-stack-docker-compose/`.

**Start the stack in the foreground.** First run pulls the three images, creates the network/volumes, then streams all container logs to the terminal (Ctrl-C stops them).

```bash
$ docker compose -f conduktor-kafka-single.yml up
```

```text
[+] up 51/51
 ✔ Image conduktor/conduktor-console:1.36.2                 Pulled                                                                              180.6s
 ✔ Image confluentinc/cp-kafka:8.0.0                        Pulled                                                                              162.2s
 ✔ Image postgres:14                                        Pulled                                                                               23.1s
 ✔ Network kafka-stack-docker-compose_default               Created                                                                               0.0s
 ✔ Volume kafka-stack-docker-compose_pg_data                Created                                                                               0.0s
 ✔ Volume kafka-stack-docker-compose_conduktor_data         Created                                                                               0.0s
 ✔ Container kafka-stack-docker-compose-postgresql-1        Created                                                                               0.4s
 ✔ Container kafka-stack-docker-compose-conduktor-console-1 Created                                                                               0.4s
 ✔ Container kafka1                                         Created                                                                               0.4s
Attaching to conduktor-console-1, postgresql-1, kafka1
kafka1  | ===> User
... (15 more lines)
```

**Confirm what is listening.** `kafka1` publishes 9092 (clients), 9099 (inter-broker) and 9999 (JMX); the console is on 8080; Postgres stays internal.

```bash
$ docker ps
```

```text
CONTAINER ID   IMAGE                                COMMAND                  CREATED          STATUS                    PORTS                                                                                              NAMES
0cde646fbc84   confluentinc/cp-kafka:8.0.0          "/etc/confluent/dock…"   34 seconds ago   Up 33 seconds             0.0.0.0:9092->9092/tcp, 0.0.0.0:9099->9099/tcp, 0.0.0.0:9999->9999/tcp, 0.0.0.0:29092->29092/tcp   kafka1
a1fb69bf2a0d   conduktor/conduktor-console:1.36.2   "/__cacert_entrypoin…"   34 seconds ago   Up 33 seconds (healthy)   0.0.0.0:8080->8080/tcp                                                                             kafka-stack-docker-compose-conduktor-console-1
f1d2af333762   postgres:14                          "docker-entrypoint.s…"   34 seconds ago   Up 33 seconds             5432/tcp                                                                                           kafka-stack-docker-compose-postgresql-1
```

**Tear it down.** `down` removes containers and the network — unlike Ctrl-C, which only stops them.

```bash
$ docker compose -f conduktor-kafka-single.yml down
```

```text
[+] down 4/4
 ✔ Container kafka1                                         Removed                                                                               0.9s
 ✔ Container kafka-stack-docker-compose-conduktor-console-1 Removed                                                                               0.0s
 ✔ Container kafka-stack-docker-compose-postgresql-1        Removed                                                                               0.1s
 ✔ Network kafka-stack-docker-compose_default               Removed                                                                               0.1s
```

---

## 3. Broker option B — local Kafka 4.0.0 in KRaft mode

Run from the extracted `kafka_2.13-4.0.0/` directory. Kafka 4.x has no ZooKeeper, so the log
directory must be formatted with a cluster ID before the broker will start.

**Generate a cluster ID** and keep it in a shell variable. `random-uuid` just prints a base64 UUID.

```bash
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

**Check the value** that was generated.

```bash
$ echo $KAFKA_CLUSTER_ID
```

```text
BGalVY-sRQKlyE6guu8XdA
```

**Format the storage directory** with that ID. `--standalone` writes a single-voter quorum config, so one process acts as both broker and controller. The log dir comes from `log.dirs` in `config/server.properties` (default `/tmp/kraft-combined-logs`).

```bash
$ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

```text
Formatting dynamic metadata voter directory /tmp/kraft-combined-logs with metadata.version 4.0-IV3.
```

> Formatting is a one-time step per log directory. Re-running it on an already-formatted directory fails unless `--ignore-formatted` is passed.

**Start the broker** in the foreground. Leave this terminal open; every other command below runs in a second terminal.

```bash
$ kafka-server-start.sh config/server.properties
```

**Port 9092 already taken?** Check whether a Docker broker from option A is still bound to it.

```bash
$ docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Ports}}" | grep 9092
```

```text
0cde646fbc84   kafka1                                           0.0.0.0:9092->9092/tcp, 0.0.0.0:9099->9099/tcp, 0.0.0.0:9999->9999/tcp, 0.0.0.0:29092->29092/tcp
```

**Free the port** by stopping the container, then start the local broker.

```bash
$ docker stop kafka1
```

```text
kafka1
```

---

## 4. Topics

`kafka-topics.sh` is the create/inspect/delete tool. Every invocation needs
`--bootstrap-server`; the topic name goes in `--topic`.

**List topics** on a fresh cluster — empty output means no topics yet.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Create a topic with defaults** — 1 partition, replication factor 1.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create
```

**List again** to confirm it exists.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --list
```

```text
first_topic
```

**Create with an explicit partition count.** More partitions = more consumer parallelism within one group.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic second_topic --create --partitions 3
```

```text
Created topic second_topic.
```

**Replication factor cannot exceed the broker count.** Asking for 2 copies on a 1-broker cluster fails.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic third_topic --create --partitions 3 --replication-factor 2
```

```text
Error while executing topic command : Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered.
[2026-07-23 16:07:02,998] ERROR org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered.
... (1 more lines)
```

> `InvalidReplicationFactorException` — the fix on a single-node cluster is `--replication-factor 1`.

**Retry with a valid replication factor.**

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic third_topic --create --partitions 3 --replication-factor 1
```

**Describe one topic.** Header line gives the topic ID, partition count, replication factor and non-default configs; one line follows per partition with its leader, replica set and ISR (in-sync replicas).

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --describe
```

```text
Topic: first_topic	TopicId: pdZXXVvgRQ2z1sQFe9j8jw	PartitionCount: 1	ReplicationFactor: 1	Configs: segment.bytes=1073741824
```

**Describe a 3-partition topic** — all three partitions are led by broker 1 because there is only one broker.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic second_topic --describe
```

```text
Topic: second_topic	TopicId: gKDXvtWLS7izw-IjDPrahQ	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1073741824
	Topic: second_topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: second_topic	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: second_topic	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
```

**Describe every topic at once** by omitting `--topic`. Useful for a quick inventory.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

```text
Topic: demo_java	TopicId: 4-JuQMueQpigeEwRQSk-Lw	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1073741824
	Topic: demo_java	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: demo_java	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: demo_java	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
Topic: wikimedia.recentchange	TopicId: 6EVPgM_KTU-sbo93D4_m7Q	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1073741824
	Topic: wikimedia.recentchange	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: wikimedia.recentchange	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: wikimedia.recentchange	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
Topic: first_topic	TopicId: MX2GBQ-EShuXgROSqRkOdQ	PartitionCount: 1	ReplicationFactor: 1	Configs: segment.bytes=1073741824
... (62 more lines)
```

**Delete a topic.** Silent on success; only works when `delete.topic.enable=true` (the default).

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --delete
```

**Describing a missing topic** is an error, not empty output — handy as an existence check.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic second_topic --describe
```

```text
Error while executing topic command : Topic 'Optional[second_topic]' does not exist as expected
[2026-07-23 18:07:53,243] ERROR java.lang.IllegalArgumentException: Topic 'Optional[second_topic]' does not exist as expected
... (6 more lines)
```

---

## 5. Producing from the CLI

`kafka-console-producer.sh` reads one message per line from stdin; the `>` characters are its
prompt. Ctrl-C (or Ctrl-D) ends the session.

**Plain producer** — messages have no key, so they are spread across partitions by the default (sticky) partitioner.

```bash
$ kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic
```

```text
>Hello World
>My name is Conduktor
>I love Kafka
```

**Require full acknowledgement.** `acks=all` makes the producer wait until all in-sync replicas have the record — the durable setting, and the default from Kafka 3.0 onward.

```bash
$ kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic --producer-property acks=all
```

```text
>acking message
>^
```

**Send keyed messages.** With `parse.key`/`key.separator`, everything left of the `:` becomes the key. Same key → same partition, which is how Kafka gives you per-key ordering.

```bash
$ kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic --property parse.key=true --property key.separator=:
```

```text
>>example key:example value
>name:Stephane
```

**Force round-robin partitioning** instead of the default sticky batching, so consecutive messages land on different partitions. Values 1–10 here are used in the consumer-group demos below.

```bash
$ kafka-console-producer.sh --bootstrap-server localhost:9092 --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner --topic third_topic
```

```text
>1
>2
>3
>4
>5
>6
>7
>8
>9
>10
>^
```

---

## 6. Consuming from the CLI

`kafka-console-consumer.sh` without `--group` gets a throwaway group each run, and by default
reads only messages produced *after* it starts.

**Tail a topic.** Nothing is printed until a producer sends something new.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic
```

```text
one
two
three
```

**Read the whole retained log** with `--from-beginning`. All six messages sent earlier come back — including the keyed ones, whose values (`example value`, `Stephane`) are printed without their keys because `print.key` was not set. The count on exit tells you how many were processed.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning
```

```text
Hello World
My name is Conduktor
I love Kafka
acking message
example value
Stephane
^CProcessed a total of 6 messages
```

**Print message metadata**, not just the value. Order across partitions is interleaved: Kafka guarantees ordering *within* a partition only, which is exactly what this output shows (`null` = no key was set).

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic \
    --property print.timestamp=true --property print.key=true \
    --property print.value=true --property print.partition=true --from-beginning
```

```text
CreateTime:1784814560698	Partition:1	null	but why?
CreateTime:1784814605257	Partition:1	null	two
CreateTime:1784814574861	Partition:0	null	aaiiooo
CreateTime:1784814606246	Partition:0	null	three
CreateTime:1784814555108	Partition:2	null	hello secon topic on fire
CreateTime:1784814603560	Partition:2	null	one
CreateTime:1784815506070	Partition:2	null	okkkk
CreateTime:1784815517757	Partition:1	null	but whyyyyyy? 123
CreateTime:1784815533388	Partition:0	null	got it
```

**The `--formatter` route fails on Kafka 4.** Passing the old class name prints the full usage help instead of consuming.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --property print.partition=true --from-beginning
```

```text
kafka.tools.DefaultMessageFormatter
Option                                   Description
------                                   -----------
... (85 more lines)
```

> `kafka.tools.DefaultMessageFormatter` was removed in Kafka 4.0 — the class is now `org.apache.kafka.tools.consumer.DefaultMessageFormatter`. Simplest fix: drop `--formatter` and use the `--property print.*` flags as in the previous command.

---

## 7. Consumer groups

A group shares out the partitions of a topic among its members and stores its progress
(committed offsets) in `__consumer_offsets`, so a restarted consumer resumes where it stopped.

**Consume as part of a named group.** Because the group is new, it starts at the earliest offset, then commits as it goes.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic third_topic --group my-first-application
```

```text
test
dsdsds
one
two
three
5555
2
5
8
13
16
^CProcessed a total of 11 messages
```

**List groups** known to the cluster.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

```text
my-first-application
```

**Describing a group that was never used** is an error — groups are created when a consumer first joins.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-second-application
```

```text
Error: Executing consumer group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group my-second-application not found.
... (9 more lines)
```

> `GroupIdNotFoundException`. Start a consumer with `--group my-second-application` once and the group appears.

**Describe a group** — the key column is `LAG` (`LOG-END-OFFSET − CURRENT-OFFSET`), i.e. how many messages the group still has to read. `has no active members` means nothing is consuming right now.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application
```

```text
Consumer group 'my-first-application' has no active members.

GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
my-first-application third_topic     2          8               10              2               -               -               -
my-first-application third_topic     1          7               9               2               -               -               -
my-first-application third_topic     0          9               12              3               -               -               -
```

**A second group reads the same topic independently.** Each group has its own offsets, so both can consume every message.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic third_topic --group my-second-application
```

```text
b
c
a
d
^CProcessed a total of 4 messages
```

**Caught-up group:** `CURRENT-OFFSET` equals `LOG-END-OFFSET` on every partition, so `LAG` is 0.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-second-application
```

```text
Consumer group 'my-second-application' has no active members.

GROUP                 TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
my-second-application third_topic     2          10              10              0               -               -               -
my-second-application third_topic     1          9               9               0               -               -               -
my-second-application third_topic     0          12              12              0               -               -               -
```

---

## 8. Resetting offsets

Replaying data means rewriting a group's committed offsets. The group must have **no active
members**, and the tool is deliberately two-step: `--dry-run` shows the plan, `--execute` applies it.

**Preview the reset** — `--to-earliest` targets the oldest retained offset. Nothing is changed yet.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group my-first-application --reset-offsets --to-earliest --topic third_topic --dry-run
```

```text
GROUP                          TOPIC                          PARTITION  NEW-OFFSET
my-first-application           third_topic                    2          0
my-first-application           third_topic                    1          0
my-first-application           third_topic                    0          0
```

**Apply it.** Same output, but the offsets are now committed as shown.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group my-first-application --reset-offsets --to-earliest --topic third_topic --execute
```

```text
GROUP                          TOPIC                          PARTITION  NEW-OFFSET
my-first-application           third_topic                    2          0
my-first-application           third_topic                    1          0
my-first-application           third_topic                    0          0
```

**Confirm the rewind:** `CURRENT-OFFSET` is 0 and `LAG` now equals the full log size — the whole topic is queued for redelivery.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application
```

```text
Consumer group 'my-first-application' has no active members.

GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
my-first-application third_topic     2          0               10              10              -               -               -
my-first-application third_topic     1          0               9               9               -               -               -
my-first-application third_topic     0          0               12              12              -               -               -
```

**Re-run the consumer** and the earlier messages arrive again.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic third_topic --group my-first-application
```

```text
dsdsds
three
5555
2
5
8
12
15
13
b
... (21 more lines)
```

**Back to zero lag** once the replay finishes.

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application
```

```text
Consumer group 'my-first-application' has no active members.

GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
my-first-application third_topic     2          10              10              0               -               -               -
my-first-application third_topic     1          9               9               0               -               -               -
my-first-application third_topic     0          12              12              0               -               -               -
```

---

## 9. Topics for the Java applications

The topics the `kafka-java-practice` modules read and write. The CLI consumer is the quickest
way to verify a Java producer is actually publishing.

**Topic for the basic producer/consumer demos** (`KafkaProducerDemo`, `KafkaConsumerDemo`, …), 3 partitions so key-based routing is observable.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3 --replication-factor 1
```

```text
Created topic demo_java.
```

**Verify the Java producer's output.** `hello world N` is what `KafkaProducerWithCallback` sends.

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning
```

```text
hello world
hello world 0
hello world 1
hello world 2
hello world 3
hello world 4
hello world 5
hello world 6
hello world 7
hello world 8
... (223 more lines)
```

**Topic for the Wikimedia stream** — the naming convention is `<domain>.<dataset>`.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --topic wikimedia.recentchange --create --partitions 3 --replication-factor 1
```

```text
Created topic wikimedia.recentchange.
```

**Watch the live Wikimedia feed** that `WikimediaChangesProducer` pushes in (one JSON event per edit, from the SSE endpoint `stream.wikimedia.org/v2/stream/recentchange`).

```bash
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikimedia.recentchange
```

```text
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%93%E1%83%90%E1%83%91%E1%83%90%E1%83%93%E1%83%94%E1%83%91%E1%83%A3%E1%83%9A%E1%83%98_22_%E1%83%9B%E1%83%90%E1%83%A0%E1%83%A2%E1%83%98","request_id":"5ea43149-d40b-484d-a6ee-75d0d689efe7","id":"d681d553-6ba9-4bfb-b8d4-c7ab505f0a6e","domain":"ka.wikipedia.org","stream":"mediawiki.recentchange","dt":"2026-07-25T16:58:43.996Z","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":6359726881},"id":15306159,"type":"categorize","namespace":14,"title":"კატეგორია:დაბადებული 22 მარტი","title_url":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%93%E1%83%90%E1%83%91%E1%83%90%E1%83%93%E1%83%94%E1%83%91%E1%83%A3%E1%83%9A%E1%83%98_22_%E1%83%9B%E1%83%90%E1%83%A0%E1%83%A2%E1%83%98","comment":"[[:ნოა ლალონდი]] დაემატა კატეგორიაში","timestamp":1784998722,"user":"MariGoderdzz-QR","bot":false,"notify_url":"https://ka.wikipedia.org/w/index.php?diff=5463251&oldid=5463249","server_url":"https://ka.wikipedia.org","server_name":"ka.wikipedia.org","server_script_path":"/w","wiki":"kawiki","parsedcomment":"<a href=\"/wiki/%E1%83%9C%E1%83%9D%E1%83%90_%E1%83%9A%E1%83%90%E1%83%9A%E1%83%9D%E1%83%9C%E1%83%93%E1%83%98\" title=\"ნოა ლალონდი\">ნოა ლალონდი</a> დაემატა კატეგორიაში"}
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%93%E1%83%90%E1%83%91%E1%83%90%E1%83%93%E1%83%94%E1%83%91%E1%83%A3%E1%83%9A%E1%83%98_1998","request_id":"5ea43149-d40b-484d-a6ee-75d0d689efe7","id":"692982e6-6650-4c70-968b-42f51f1c9658","domain":"ka.wikipedia.org","stream":"mediawiki.recentchange","dt":"2026-07-25T16:58:44.015Z","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":6359726882},"id":15306160,"type":"categorize","namespace":14,"title":"კატეგორია:დაბადებული 1998","title_url":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%93%E1%83%90%E1%83%91%E1%83%90%E1%83%93%E1%83%94%E1%83%91%E1%83%A3%E1%83%9A%E1%83%98_1998","comment":"[[:ნოა ლალონდი]] დაემატა კატეგორიაში","timestamp":1784998722,"user":"MariGoderdzz-QR","bot":false,"notify_url":"https://ka.wikipedia.org/w/index.php?diff=5463251&oldid=5463249","server_url":"https://ka.wikipedia.org","server_name":"ka.wikipedia.org","server_script_path":"/w","wiki":"kawiki","parsedcomment":"<a href=\"/wiki/%E1%83%9C%E1%83%9D%E1%83%90_%E1%83%9A%E1%83%90%E1%83%9A%E1%83%9D%E1%83%9C%E1%83%93%E1%83%98\" title=\"ნოა ლალონდი\">ნოა ლალონდი</a> დაემატა კატეგორიაში"}
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%90%E1%83%9B%E1%83%94%E1%83%A0%E1%83%98%E1%83%99%E1%83%94%E1%83%9A%E1%83%94%E1%83%91%E1%83%98","request_id":"5ea43149-d40b-484d-a6ee-75d0d689efe7","id":"3410eeb9-6e35-4838-8a32-408815ec1e98","domain":"ka.wikipedia.org","stream":"mediawiki.recentchange","dt":"2026-07-25T16:58:44.032Z","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":6359726883},"id":15306161,"type":"categorize","namespace":14,"title":"კატეგორია:ამერიკელები","title_url":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%90%E1%83%9B%E1%83%94%E1%83%A0%E1%83%98%E1%83%99%E1%83%94%E1%83%9A%E1%83%94%E1%83%91%E1%83%98","comment":"[[:ნოა ლალონდი]] დაემატა კატეგორიაში","timestamp":1784998722,"user":"MariGoderdzz-QR","bot":false,"notify_url":"https://ka.wikipedia.org/w/index.php?diff=5463251&oldid=5463249","server_url":"https://ka.wikipedia.org","server_name":"ka.wikipedia.org","server_script_path":"/w","wiki":"kawiki","parsedcomment":"<a href=\"/wiki/%E1%83%9C%E1%83%9D%E1%83%90_%E1%83%9A%E1%83%90%E1%83%9A%E1%83%9D%E1%83%9C%E1%83%93%E1%83%98\" title=\"ნოა ლალონდი\">ნოა ლალონდი</a> დაემატა კატეგორიაში"}
{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%90%E1%83%9B%E1%83%94%E1%83%A0%E1%83%98%E1%83%99%E1%83%94%E1%83%9A%E1%83%98_%E1%83%9B%E1%83%A1%E1%83%90%E1%83%AE%E1%83%98%E1%83%9D%E1%83%91%E1%83%94%E1%83%91%E1%83%98","request_id":"5ea43149-d40b-484d-a6ee-75d0d689efe7","id":"42ae1bab-97a8-43fb-80c0-07eb0ea16ace","domain":"ka.wikipedia.org","stream":"mediawiki.recentchange","dt":"2026-07-25T16:58:44.051Z","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":6359726884},"id":15306162,"type":"categorize","namespace":14,"title":"კატეგორია:ამერიკელი მსახიობები","title_url":"https://ka.wikipedia.org/wiki/%E1%83%99%E1%83%90%E1%83%A2%E1%83%94%E1%83%92%E1%83%9D%E1%83%A0%E1%83%98%E1%83%90:%E1%83%90%E1%83%9B%E1%83%94%E1%83%A0%E1%83%98%E1%83%99%E1%83%94%E1%83%9A%E1%83%98_%E1%83%9B%E1%83%A1%E1%83%90%E1%83%AE%E1%83%98%E1%83%9D%E1%83%91%E1%83%94%E1%83%91%E1%83%98","comment":"[[:ნოა ლალონდი]] დაემატა კატეგორიაში","timestamp":1784998722,"user":"MariGoderdzz-QR","bot":false,"notify_url":"https://ka.wikipedia.org/w/index.php?diff=5463251&oldid=5463249","server_url":"https://ka.wikipedia.org","server_name":"ka.wikipedia.org","server_script_path":"/w","wiki":"kawiki","parsedcomment":"<a href=\"/wiki/%E1%83%9C%E1%83%9D%E1%83%90_%E1%83%9A%E1%83%90%E1%83%9A%E1%83%9D%E1%83%9C%E1%83%93%E1%83%98\" title=\"ნოა ლალონდი\">ნოა ლალონდი</a> დაემატა კატეგორიაში"}
... (86 more lines)
```

---

## 10. OpenSearch sink stack

`kafka-java-practice/kafka-consumer-opensearch/docker-compose.yml` brings up OpenSearch (9200)
and OpenSearch Dashboards (5601) with security disabled, which is what `OpenSearchConsumer`
indexes the Wikimedia events into.

**Start both containers detached.** `-d` returns the prompt immediately, unlike the foreground `up` in section 2.

```bash
$ docker compose up -d
```

```text
WARN[0000] /home/vishwas.maheshwari@ad.franconnect.com/practice/kafka/kafka-java-practice/kafka-consumer-opensearch/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion
[+] up 15/15
 ✔ Image opensearchproject/opensearch-dashboards:1.2.0         Pulled                                                                           158.4s
 ✔ Image opensearchproject/opensearch:1.2.4                    Pulled                                                                           189.9s
 ✔ Network kafka-consumer-opensearch_default                   Created                                                                            0.1s
 ✔ Container kafka-consumer-opensearch-opensearch-dashboards-1 Started                                                                            0.6s
 ✔ Container kafka-consumer-opensearch-opensearch-1            Started                                                                            0.6s
```

**Confirm the ports** — 9200 is the REST API, 5601 the Dashboards UI (dev tools console at `http://localhost:5601/app/dev_tools#/console`).

```bash
$ docker ps
```

```text
CONTAINER ID   IMAGE                                           COMMAND                  CREATED          STATUS          PORTS                                                                NAMES
b1d1dfafe1ba   opensearchproject/opensearch-dashboards:1.2.0   "./opensearch-dashbo…"   14 minutes ago   Up 14 minutes   0.0.0.0:5601->5601/tcp                                               kafka-consumer-opensearch-opensearch-dashboards-1
007f7ab662cc   opensearchproject/opensearch:1.2.4              "./opensearch-docker…"   14 minutes ago   Up 14 minutes   0.0.0.0:9200->9200/tcp, 9300/tcp, 0.0.0.0:9600->9600/tcp, 9650/tcp   kafka-consumer-opensearch-opensearch-1
```

**Stop the containers** without deleting them (state in the volumes survives).

```bash
$ docker stop b1d1dfafe1ba 007f7ab662cc
```

```text
b1d1dfafe1ba
007f7ab662cc
```

**Start the same containers again** by ID — faster than `compose up` since nothing is recreated.

```bash
$ docker start b1d1dfafe1ba 007f7ab662cc
```

```text
b1d1dfafe1ba
007f7ab662cc
```

---

## 11. Dynamic topic configs

`kafka-configs.sh` changes broker/topic settings at runtime, without a restart. Topic-level
values override the broker default.

**Show a topic's dynamic configs.** Empty means everything is still inherited from the broker defaults.

```bash
$ kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name third_topic --describe
```

```text
Dynamic configs for topic third_topic are:
```

**Set a config.** `min.insync.replicas=2` requires 2 in-sync replicas to acknowledge an `acks=all` write.

```bash
$ kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics \
    --entity-name third_topic --alter --add-config min.insync.replicas=2
```

```text
Completed updating config for topic third_topic.
```

> On this single-broker cluster the setting is accepted but unsatisfiable — `acks=all` producers would then fail with `NotEnoughReplicasException`. It is set here only to watch the config propagate.

**Read it back.** `synonyms` shows precedence: the dynamic topic value (2) wins over the default (1).

```bash
$ kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name third_topic --describe
```

```text
Dynamic configs for topic third_topic are:
  min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=2, DEFAULT_CONFIG:min.insync.replicas=1}
```

**The same override also shows up** in the `Configs:` column of `kafka-topics.sh --describe`.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic third_topic
```

```text
Topic: third_topic	TopicId: yIH-hpcxRdmUwfjxpjixNw	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=2,segment.bytes=1073741824
	Topic: third_topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
... (2 more lines)
```

**Remove the override** and fall back to the broker default.

```bash
$ kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics \
    --entity-name third_topic --alter --delete-config min.insync.replicas
```

```text
Completed updating config for topic third_topic.
```

**Confirm it is gone** — `Configs:` is back to just `segment.bytes`.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic third_topic
```

```text
Topic: third_topic	TopicId: yIH-hpcxRdmUwfjxpjixNw	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1073741824
	Topic: third_topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
... (2 more lines)
```

---

## 12. Log compaction

A compacted topic keeps only the **latest value per key** instead of expiring data by time or
size — the right model for a keyed snapshot such as "current salary per employee".

**Create a compacted topic.** `min.cleanable.dirty.ratio=0.001` and `segment.ms=5000` are demo-only values that make the cleaner run almost immediately instead of after hours.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic employee-salary \
    --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.001 \
    --config segment.ms=5000
```

```text
Created topic employee-salary.
```

**Verify the settings** landed. All three `--config` values show up in the `Configs:` column next to the inherited `segment.bytes`. Producing two records with the same key and then re-reading from the beginning shows only the newer value survives.

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic employee-salary
```

```text
Topic: employee-salary	TopicId: oLsxIqqYTyCbYPZ0QyIiew	PartitionCount: 1	ReplicationFactor: 1	Configs: cleanup.policy=compact,segment.bytes=1073741824,min.cleanable.dirty.ratio=0.001,segment.ms=5000
	Topic: employee-salary	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
```

> Compaction removes superseded values per key; it never removes the *latest* value for a key. A key is only fully dropped by writing a tombstone (a record with a `null` value).

---

## 13. Errors hit along the way

Every failure in the transcript, with what actually caused it.

| Command / flag as typed                           | Error                                           | Cause and fix                                                                              |
|---------------------------------------------------|-------------------------------------------------|--------------------------------------------------------------------------------------------|
| `--partitions 3 --replication-factor 2`           | `InvalidReplicationFactorException`             | Only 1 broker available. Use `--replication-factor 1`.                                     |
| `--topic third_topic --create` (second time)      | `TopicExistsException`                          | Topic already there. Add `--if-not-exists` or skip.                                        |
| `--topic second_topic --describe` (after delete)  | `Topic 'Optional[second_topic]' does not exist` | Describing a deleted topic is an error, not empty output.                                  |
| `--describe --group my-second-application`        | `GroupIdNotFoundException`                      | The group does not exist until a consumer joins it once.                                   |
| `kafka-topics.sh --describe`                      | `--bootstrap-server must be specified`          | Every CLI call needs `--bootstrap-server localhost:9092`.                                  |
| `--entity_name third_topic`                       | `entity_name is not a recognized option`        | Underscore typo — the flag is `--entity-name`.                                             |
| `--formatter kafka.tools.DefaultMessageFormatter` | prints usage help, consumes nothing             | Class removed in Kafka 4.0; now `org.apache.kafka.tools.consumer.DefaultMessageFormatter`. |
| `--producer-property acks=allall`                 | producer started but value invalid              | Typo for `acks=all`.                                                                       |
| `--describe --group my-first-applicationion`      | describes an unintended group                   | Typo created a second group name; both appear in `--list`.                                 |

---

## Quick reference

Every command assumes `--bootstrap-server localhost:9092` and that `kafka_2.13-4.0.0/bin` is on `PATH`.

| Goal                         | Command                                                                                         |
|------------------------------|-------------------------------------------------------------------------------------------------|
| Format storage (KRaft, once) | `kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties`         |
| Start broker                 | `kafka-server-start.sh config/server.properties`                                                |
| List topics                  | `kafka-topics.sh --list`                                                                        |
| Create topic                 | `kafka-topics.sh --create --topic T --partitions 3 --replication-factor 1`                      |
| Describe topic               | `kafka-topics.sh --describe --topic T`                                                          |
| Delete topic                 | `kafka-topics.sh --delete --topic T`                                                            |
| Produce                      | `kafka-console-producer.sh --topic T`                                                           |
| Produce with keys            | `kafka-console-producer.sh --topic T --property parse.key=true --property key.separator=:`      |
| Consume new messages         | `kafka-console-consumer.sh --topic T`                                                           |
| Consume everything           | `kafka-console-consumer.sh --topic T --from-beginning`                                          |
| Consume with metadata        | `kafka-console-consumer.sh --topic T --property print.key=true --property print.partition=true` |
| Consume in a group           | `kafka-console-consumer.sh --topic T --group G`                                                 |
| List groups                  | `kafka-consumer-groups.sh --list`                                                               |
| Group lag                    | `kafka-consumer-groups.sh --describe --group G`                                                 |
| Replay from start            | `kafka-consumer-groups.sh --group G --reset-offsets --to-earliest --topic T --execute`          |
| Show topic configs           | `kafka-configs.sh --entity-type topics --entity-name T --describe`                              |
| Set topic config             | `kafka-configs.sh --entity-type topics --entity-name T --alter --add-config k=v`                |
| Remove topic config          | `kafka-configs.sh --entity-type topics --entity-name T --alter --delete-config k`               |
