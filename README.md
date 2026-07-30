# Apache Kafka Practice Repository

A hands-on learning repository for Apache Kafka covering producers, consumers, streams, and operational tools. Includes course materials, CLI walkthroughs, Docker setups, and Java examples.

**Transcript Period:** 22–29 Jul 2026 | **Kafka Version:** 4.0.0 (KRaft mode)

---

## 📁 Repository Structure

```
.
├── README.md                              # This file
├── .gitignore                             # Git ignore rules
├── commands.md                            # Annotated CLI walkthrough (897 lines)
├── commands.txt                           # Raw terminal transcript (3,133 lines)
├── notes.md                               # Reference links (admin, monitoring, replication)
│
├── kafka_2.13-4.0.0/                     # Kafka binary (extracted)
│   ├── bin/                              # Executable scripts
│   ├── config/                           # Configuration templates
│   └── libs/                             # JAR dependencies
│
├── kafka-java-practice/                  # Gradle multi-module project (211 prompts logged)
│   ├── kafka-basic/                      # Producers, consumers, advanced patterns
│   │   └── src/main/java/org/example/
│   │       ├── KafkaProducerDemo.java
│   │       ├── KafkaProducerWithCallback.java
│   │       ├── KafkaProducerWithKeys.java
│   │       ├── KafkaConsumerDemo.java
│   │       ├── KafkaConsumerWithGracefulShutDown.java
│   │       ├── KafkaConsumerCooperative.java
│   │       └── advanced/                 # Rebalance listeners, assign/seek
│   │
│   ├── kafka-producer-wikimedia/         # Real-time SSE consumer → Kafka
│   │   └── src/main/java/org/example/
│   │       ├── WikimediaChangesProducer.java
│   │       └── WikimediaChangeHandler.java
│   │
│   └── kafka-consumer-opensearch/        # Kafka consumer → OpenSearch indexing
│       ├── docker-compose.yml            # OpenSearch + Dashboards
│       └── src/main/java/org/example/
│           └── OpenSearchConsumer.java
│
├── kafka-beginners-course/               # Conduktor course repo (git clone)
│   ├── kafka-basics/
│   ├── kafka-producer-wikimedia/
│   ├── kafka-consumer-opensearch/
│   ├── kafka-streams-wikimedia/
│   └── README.md
│
├── kafka-code-part-1/                    # CLI scripts and configs
│   ├── 1-kafka-cli/                      # Topic, producer, consumer, group commands
│   ├── 2-kafka-extended/                 # Kafka Connect, Schema Registry
│   ├── 3-kafka-topic-configurations/     # Log compaction, topic configs
│   ├── allcourse.txt                     # HTML: bonus lecture with course links
│   ├── courses.md                        # Extracted course list (37 courses)
│   └── README.txt
│
├── kafka-stack-docker-compose/           # Conduktor docker-compose stacks
│   ├── zk-single-kafka-single.yml        # ZooKeeper + 1 broker
│   ├── zk-multiple-kafka-multiple.yml    # Multi-broker, multi-ZK
│   ├── conduktor-kafka-single.yml        # KRaft + web console
│   └── full-stack.yml
│
├── images/                               # 196 screenshots (22–29 Jul 2026)
│   └── Screenshot from YYYY-MM-DD HH-MM-SS.png
│
├── kafka-beginners.pptx                  # All 196 images as slides (62 MB, gitignored)
├── kafka-beginners.pdf                   # PDF version (22 MB, gitignored)
│
├── Install+Kafka+Diagram+v3.pdf          # Architecture diagrams
├── kafka_2.13-4.0.0.tgz                  # Kafka binary archive
└── kafka-for-beginners-code-20250813.zip # Course code archive
```

---

## 🚀 Quick Start

### Prerequisites

- **Java 21+** (required for Kafka 4.0.0)
- **Docker & Docker Compose** (for broker + OpenSearch stacks)
- **Gradle 8.14.5+** (for Java projects; wrapper included)
- Kafka binary extracted to `kafka_2.13-4.0.0/`

### Option A: Broker via Docker (Fastest)

```bash
cd kafka-stack-docker-compose
docker compose -f conduktor-kafka-single.yml up

# Web console: http://localhost:8080
# Broker:      localhost:9092
```

### Option B: Local Kafka 4.0.0 (KRaft)

```bash
cd kafka_2.13-4.0.0

# Generate cluster ID
export KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Format storage (one-time)
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

# Start broker
bin/kafka-server-start.sh config/server.properties
```

### Create a Topic & Produce

```bash
# From a second terminal, add bin/ to PATH:
export PATH="$(pwd)/kafka_2.13-4.0.0/bin:$PATH"

# Create topic
kafka-topics.sh --bootstrap-server localhost:9092 --topic demo --create --partitions 3

# Produce
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo
> Hello Kafka
> (Ctrl-D to exit)

# Consume
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo --from-beginning
```

---

## 📚 What's Included

### CLI Command Reference

**`commands.md`** (897 lines)  
A fully annotated walkthrough of every command actually run, grouped into 13 sections:

1. Getting the course material (`git clone`)
2. Broker option A — Docker (Conduktor stack)
3. Broker option B — KRaft (local)
4. Topics (create/list/describe/delete)
5. Producing (plain, keyed, round-robin partitioner)
6. Consuming (tail, `--from-beginning`, metadata)
7. Consumer groups (`--group`, LAG, `--describe`)
8. Resetting offsets (dry-run → execute → replay)
9. Topics for Java apps (`demo_java`, `wikimedia.recentchange`)
10. OpenSearch sink (docker, container lifecycle)
11. Dynamic topic configs (`--alter`, `--add-config`, `--delete-config`)
12. Log compaction (`cleanup.policy=compact`)
13. Errors hit (9 failures with cause & fix)

Each entry includes:
- Plain-English explanation
- The actual command
- Real verbatim output (trimmed for length)
- Notes on edge cases

### Java Examples

**`kafka-java-practice/`** (Gradle multi-module)

**kafka-basic:**
- `KafkaProducerDemo` — send records asynchronously
- `KafkaProducerWithCallback` — track send success/failure
- `KafkaProducerWithKeys` — keyed messages for partition routing
- `KafkaConsumerDemo` — basic poll loop
- `KafkaConsumerWithGracefulShutDown` — shutdown hook, `WakeupException`
- `KafkaConsumerCooperative` — static vs. cooperative rebalancing
- Advanced: `ConsumerDemoRebalanceListener`, `ConsumerDemoAssignSeek`, `ConsumerDemoThreads`

**kafka-producer-wikimedia:**
- Stream Wikipedia edits (SSE) into Kafka
- Idempotent producer (`enable.idempotence`, `acks=all`, `retries=MAX_INT`)
- High-throughput tuning (`linger.ms`, `batch.size`, `compression.type=snappy`)

**kafka-consumer-opensearch:**
- Consumer group: `consumer-opensearch`
- Index JSON events from `wikimedia.recentchange` into OpenSearch
- Idempotent indexing (topic + partition + offset as document ID)
- Graceful shutdown with offset commit

Build & run:
```bash
cd kafka-java-practice
./gradlew compileJava
./gradlew kafka-basic:run --args="KafkaProducerDemo"
```

### Screenshots & Presentations

- **`images/`** — 196 screenshots spanning 22–29 Jul 2026 (ordered by capture time)
- **`kafka-beginners.pptx`** — One-click slide deck (197 slides: 1 title + 196 images)
- **`kafka-beginners.pdf`** — PDF version (searchable by timestamp caption)
- **PDF (Google Drive):** [View / Download](https://drive.google.com/file/d/1VyeOZOVIIWE7NO2G0mah8qzc2anavKJy/view?usp=sharing) — both files are gitignored (large binaries), so this is the shareable copy

### Course Links

**`courses.md`** (extracted from bonus lecture)

37 courses across 4 categories:
- **Apache Kafka Series** (7 courses: Streams, Connect, KSQL, Security, Monitoring, etc.)
- **AWS Certifications** (26 certifications + practice exams)
- **Claude / Anthropic Certifications** (4 practice exam courses)

Discount code: `JUL_26_GET_STARTED`

### Documentation

- **`notes.md`** — Reference links (admin, monitoring, replication, schema registry, etc.)
- **`Install+Kafka+Diagram+v3.pdf`** — Architecture diagrams
- **`kafka-code-part-1/`** — CLI scripts organized by topic (topic management, Connect, log compaction)

---

## 🛠 Common Tasks

### List Topics

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Describe Topic (Partitions, Leader, Replicas, ISR)

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic demo
```

### Consumer Group Lag

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
```

### Reset Offsets to Earliest (Replay)

```bash
# Dry-run (show what would change)
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --reset-offsets --to-earliest --topic demo --dry-run

# Execute
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --reset-offsets --to-earliest --topic demo --execute
```

### Dynamic Topic Config (Alter)

```bash
# Set min.insync.replicas for durability
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name demo \
  --alter --add-config min.insync.replicas=2

# View
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name demo --describe

# Remove
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name demo \
  --alter --delete-config min.insync.replicas
```

### Start OpenSearch Consumer

```bash
cd kafka-java-practice/kafka-consumer-opensearch
docker compose up -d
./gradlew run
```

Then visit OpenSearch Dashboards: **http://localhost:5601/app/dev_tools#/console**

---

## 📖 Learning Path

1. **Understand the basics:** Read `commands.md` sections 1–4 (setup, topics)
2. **Produce & consume:** Sections 5–6; run CLI commands yourself
3. **Advanced:** Sections 7–12 (groups, offsets, configs, compaction)
4. **Review errors:** Section 13 (common pitfalls)
5. **Java examples:** Explore `kafka-java-practice/` modules
6. **Wikimedia pipeline:** Run `WikimediaChangesProducer` + `OpenSearchConsumer`

---

## 🔗 Resources

- **Kafka Docs:** https://kafka.apache.org/
- **Confluent:** https://www.confluent.io/
- **Conduktor:** https://www.conduktor.io/ (free console + UI)
- **Kafkademy:** https://kafkademy.com/ (interactive learning)

---

## 📝 Notes

- **Kafka Version:** 4.0.0 (KRaft mode, no ZooKeeper)
- **Single Broker:** All examples run on a single-broker cluster (`localhost:9092`)
- **Replication Factor:** Limited to 1 due to single broker
- **Transcript:** `commands.txt` and `commands.md` capture 211 shell prompts over 8 days of practice
- **Screenshots:** 196 images (1362×760–767 px) from terminal + IDE + web consoles

---

## 🐛 Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `--bootstrap-server must be specified` | Missing required flag | Add `--bootstrap-server localhost:9092` to every command |
| `TopicExistsException` | Creating topic that already exists | Omit `--create` or use `--if-not-exists` |
| `GroupIdNotFoundException` | Consumer group never had a member | Run a consumer with `--group GROUP_NAME` once to create it |
| `InvalidReplicationFactorException` | RF > broker count | Use `--replication-factor 1` on single-broker clusters |
| `kafka.tools.DefaultMessageFormatter` not found | Kafka 4.0 removed the class | Use `--property print.key=true --property print.partition=true` instead |

---

## 📄 License & Attribution

This repository documents practice with **Apache Kafka** (open source) and Conduktor's **Kafka for Beginners** course materials.

**Course Instructor:** Stephane Maarek  
**Course Resources:** https://links.datacumulus.com/kafka-beginners-coupon  
**Discount Code:** JUL_26_GET_STARTED

---

**Created:** 22–29 Jul 2026  
**Last Updated:** 30 Jul 2026
