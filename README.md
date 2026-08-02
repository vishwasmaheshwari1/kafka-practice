# Apache Kafka Practice Repository

A hands-on learning repository for Apache Kafka covering producers, consumers, Avro
serialization, Schema Registry, the REST Proxy, and operational tools. Includes course
materials, an annotated CLI walkthrough, Docker setups, and Java examples across two
practice arcs.

**Course 1 — Kafka Fundamentals:** 22–29 Jul 2026
**Course 2 — Schema Registry & Avro:** 30 Jul–2 Aug 2026

**Kafka Version:**
- 4.0.0 (KRaft mode) for CLI/Java work
- `fast-data-dev` bundle (Kafka 3.9-L0) for the Schema Registry course

---

## 📁 Repository Structure

```
.
├── README.md                              # This file
├── .gitignore                             # Git ignore rules
├── commands.md                            # Annotated CLI walkthrough (1265 lines, 20 sections)
├── commands.txt                           # Raw terminal transcript (gitignored)
├── courses.md                             # 37 related courses, extracted from a bonus lecture page
├── notes.md                               # Reference links (admin, monitoring, Avro, Schema Registry, REST Proxy)
│
├── kafka-commands/                        # CLI scripts by topic (renamed from kafka-code-part-1)
│   ├── 1-kafka-cli/                       # Topic, producer, consumer, group commands
│   ├── 2-kafka-extended/                 # Kafka Connect, Schema Registry configs
│   └── 3-kafka-topic-configurations/     # Log compaction, topic configs
│
├── kafka-java/                            # Gradle multi-module project (renamed from kafka-java-practice)
│   ├── kafka-basic/                      # Producers, consumers, advanced patterns
│   │   └── src/main/java/org/example/
│   │       ├── KafkaProducerDemo.java
│   │       ├── KafkaProducerWithCallback.java
│   │       ├── KafkaProducerWithKeys.java
│   │       ├── KafkaConsumerDemo.java
│   │       ├── KafkaConsumerWithGracefulShutDown.java
│   │       ├── KafkaConsumerCooperative.java
│   │       └── advanced/                 # Rebalance listeners, assign/seek
│   ├── kafka-producer-wikimedia/         # Real-time SSE consumer → Kafka
│   └── kafka-consumer-opensearch/        # Kafka consumer → OpenSearch indexing
│
├── kafka-avro/                            # Gradle multi-module Avro practice (own project, own .gitignore)
│   ├── avro-practice/                    # GenericRecord, SpecificRecord, reflection, schema evolution
│   ├── kafka-avro-1/                     # Avro producer/consumer v1 (Confluent serializer + Schema Registry)
│   ├── kafka-avro-2/                     # Avro producer/consumer v2 (schema evolution: v1 → v2)
│   ├── avro-tools-1.11.5.jar             # Avro CLI tool (gitignored, matches *.jar)
│   └── *.avro                            # Generated example output — gitignored, regenerated on each run
│
├── kafka-schema-registry-avro/            # Confluent Schema Registry & Avro course exercises
│   ├── 0-avro-schemas/                   # .avsc schema files (primitive, customer, schema evolution)
│   ├── 1-avro-java/                      # avro-tools.sh
│   ├── 2-start-kafka/                    # docker-compose.yml (fast-data-dev all-in-one image)
│   ├── 3-schema-registry/                # kafka-avro-console-producer/consumer walkthrough
│   └── 4-rest-proxy/                     # REST Proxy topic setup + Insomnia request collection
│
├── kafka-beginners-course/                # Conduktor course repo, git clone (gitignored)
├── kafka-avro-course/                     # Confluent Schema Registry & Avro course repo, git clone (gitignored)
├── kafka-stack-docker-compose/            # Conduktor docker-compose stacks, git clone (gitignored)
│
├── kafka_2.13-4.0.0/                     # Kafka binary, extracted (gitignored)
├── kafka_2.13-4.0.0.tgz                  # Kafka binary archive (gitignored)
├── kafka-for-beginners-code-20250813.zip # Course code archive (gitignored)
├── code.zip                               # Source zip for kafka-schema-registry-avro/ (gitignored)
├── Insomnia.Core-13.1.0.deb              # REST client installer, used for the REST Proxy exercises (gitignored)
│
├── images/                                # Current screenshot batch — 109 shots, 30 Jul–2 Aug 2026 (gitignored)
│   └── Screenshot from YYYY-MM-DD HH-MM-SS.png
│
├── kafka-beginners.pptx                   # Deck 1: 197 slides (196 screenshots + title), 22–29 Jul 2026 (gitignored, 62 MB)
├── kafka-beginners.pdf                    # Deck 1, PDF version (gitignored, 22 MB)
├── kafka-schema-registry.pptx             # Deck 2: 110 slides (109 screenshots + title), 30 Jul–2 Aug 2026 (gitignored, 21 MB)
├── kafka-schema-registry.pdf              # Deck 2, PDF version (gitignored, 10 MB)
│
└── Install+Kafka+Diagram+v3.pdf          # Architecture diagrams
```

> **Renames to know about:** `kafka-code-part-1/` → `kafka-commands/`, and
> `kafka-java-practice/` → `kafka-java/`. If you're looking for either by its old name,
> it moved, it wasn't deleted.
>
> **`images/` is a rolling folder**, not an archive — it holds whichever screenshot batch
> is currently in progress. The 196-shot batch behind `kafka-beginners.pptx` and
> `kafka-beginners.pdf` was exported and cleared before the 109-shot Schema Registry
> batch began; only the latter
> is on disk now. Both decks remain valid — each is a self-contained snapshot at build
> time.

---

## 🚀 Quick Start

### Prerequisites

- **Java 21+** (Kafka 4.0.0 CLI/Java work) — the Schema Registry course's `fast-data-dev`
  container brings its own Java 8, no local install needed for that half
- **Docker & Docker Compose** (all broker options, OpenSearch, Schema Registry)
- **Gradle 8.14.5+** (wrapper included in `kafka-java/` and `kafka-avro/`)
- Kafka binary extracted to `kafka_2.13-4.0.0/` (for the local KRaft option)

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

### Option C: Schema Registry course stack (Kafka + Schema Registry + Connect + REST Proxy, one container)

```bash
cd kafka-schema-registry-avro/2-start-kafka
docker compose up -d

# Broker:          localhost:9092
# Schema Registry: http://localhost:8081
# Web UI:           http://localhost:3030
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

**`commands.md`** (1265 lines) — a fully annotated walkthrough of every command
actually run, in two parts:

**Part 1 — Kafka Fundamentals (sections 1–13):**
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

**Part 2 — Schema Registry & Avro (sections 14–20):**
14. Starting the Schema Registry stack (`fast-data-dev` all-in-one container)
15. Producing/consuming Avro records (`kafka-avro-console-producer/consumer`, schema validation)
16. Schema evolution (adding a field with a default)
17. Container lifecycle troubleshooting (image tags, `--volumes`, `docker exec` flag order, port conflicts)
18. A second topic — `customer-avro`
19. Installing Insomnia (REST client for the REST Proxy exercises)
20. New errors in this segment

Each entry includes a plain-English explanation, the actual command, real verbatim
output (trimmed for length), and notes on edge cases.

### Java Examples

**`kafka-java/`** — basics, Wikimedia streaming, OpenSearch sink:

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

```bash
cd kafka-java
./gradlew compileJava
./gradlew kafka-basic:run --args="KafkaProducerDemo"
```

**`kafka-avro/`** — Avro serialization forms and schema evolution:

**avro-practice:**
- `GenericRecordExample` — read/write Avro without generated classes
- `SpecificRecordExample` — read/write using Avro-generated Java classes
- `ReflectionExamples` (+ `ReflectedCustomer`) — read/write via Java reflection, no schema/generated class needed
- `SchemaEvolutionExamples` — demonstrates adding a field across schema versions

**kafka-avro-1 and kafka-avro-2:**
- A Kafka producer/consumer pair per schema version, using the Confluent Avro serializer against a live Schema Registry
- `kafka-avro-1` on `customer-v1.avsc`
- `kafka-avro-2` on the evolved `customer-v2.avsc`

```bash
cd kafka-avro
./gradlew avro-practice:run -PmainClass=org.example.generic.GenericRecordExample
```

> Running these examples regenerates `*.avro` output files and Avro-generated Java
> sources under each module's `build/` — both are gitignored, so `git status` should
> stay clean after a run.

### Schema Registry & REST Proxy Course

**`kafka-schema-registry-avro/`** — Confluent's Schema Registry & Avro course exercises,
built around the `fast-data-dev` bundled image (Kafka + Schema Registry + Connect + REST
Proxy in one container):

- `0-avro-schemas/` — primitive and record schemas, including a schema-evolution pair
  (`2-customer-upgraded-*.avsc`)
- `2-start-kafka/docker-compose.yml` — the one-container stack
- `3-schema-registry/` — `kafka-avro-console-producer/consumer` walkthrough
- `4-rest-proxy/` — topic setup + `rest-proxy-insomnia.json`, a ready-made Insomnia
  request collection for exercising the REST Proxy over HTTP

See `commands.md` sections 14–20 for the full annotated run, including the port
conflict between the REST Proxy and `fast-data-dev`'s own bundled proxy, and why
`docker exec` flag order matters.

### Screenshots & Presentations

Two independent decks, one per course:

- **`kafka-beginners.pptx`**
  **`kafka-beginners.pdf`**
  — 197 slides (196 screenshots + title), 22–29 Jul 2026.
  Both files are gitignored; a shareable copy of the PDF is on
  [Google Drive](https://drive.google.com/file/d/1VyeOZOVIIWE7NO2G0mah8qzc2anavKJy/view?usp=sharing).
- **`kafka-schema-registry.pptx`**
  **`kafka-schema-registry.pdf`**
  — 110 slides (109 screenshots + title), 30 Jul–2 Aug 2026.

Both are ordered ascending by the timestamp embedded in each screenshot's filename, one
screenshot per slide, with a date/time caption and a running counter.

### Course Links

**`courses.md`** — 37 related courses across 4 categories (Apache Kafka Series, AWS
Certifications, Claude/Anthropic Certifications), extracted from a bonus-lecture page.
Discount code: `JUL_26_GET_STARTED`.

### Documentation

- **`notes.md`** — reference links: admin/monitoring/replication, broker/topic configs,
  Schema Registry, Avro, Kafka REST Proxy, Insomnia.
- **`Install+Kafka+Diagram+v3.pdf`** — architecture diagrams.
- **`kafka-commands/`** — CLI scripts organized by topic (topic management, Connect, log
  compaction).

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
cd kafka-java/kafka-consumer-opensearch
docker compose up -d
./gradlew run
```

Then visit OpenSearch Dashboards: **http://localhost:5601/app/dev_tools#/console**

### Produce/Consume an Avro Record Against Schema Registry

```bash
# Requires the Schema Registry stack (Option C above) running
docker run -it --rm --net=host confluentinc/cp-schema-registry:3.3.1 bash

kafka-avro-console-producer --broker-list localhost:9092 --topic test-avro \
  --property schema.registry.url=http://127.0.0.1:8081 \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
> {"f1": "value1"}

kafka-avro-console-consumer --topic test-avro --bootstrap-server 127.0.0.1:9092 \
  --property schema.registry.url=http://127.0.0.1:8081 --from-beginning
```

---

## 📖 Learning Path

1. **Understand the basics:** Read `commands.md` sections 1–4 (setup, topics)
2. **Produce & consume:** Sections 5–6; run CLI commands yourself
3. **Advanced:** Sections 7–12 (groups, offsets, configs, compaction)
4. **Review errors:** Section 13 (common pitfalls)
5. **Java examples:** Explore `kafka-java/` modules
6. **Wikimedia pipeline:** Run `WikimediaChangesProducer` + `OpenSearchConsumer`
7. **Schema Registry & Avro:** `commands.md` sections 14–16 — start the stack, produce/consume
   Avro, evolve a schema
8. **Avro in Java:** Explore `kafka-avro/` — generic vs. specific vs. reflection, then the
   producer/consumer pair in `kafka-avro-1`/`kafka-avro-2`
9. **REST Proxy:** `commands.md` sections 17–19, then exercise `4-rest-proxy/` with Insomnia
10. **Review new errors:** Section 20

---

## 🔗 Resources

- **Kafka Docs:** https://kafka.apache.org/
- **Confluent:** https://www.confluent.io/
- **Conduktor:** https://www.conduktor.io/ (free console + UI)
- **Kafkademy:** https://kafkademy.com/ (interactive learning)
- **Avro Docs:** https://avro.apache.org/docs/
- **Schema Registry Docs:** https://docs.confluent.io/platform/current/schema-registry/index.html
- **Kafka REST Proxy Docs:** https://docs.confluent.io/platform/current/kafka-rest/index.html
- **Insomnia:** https://insomnia.rest/

---

## 📝 Notes

- **Kafka Version:**
  - 4.0.0 (KRaft mode, no ZooKeeper) for the CLI/Java course
  - `lensesio/fast-data-dev:3.9.0` (bundled Kafka 3.9-L0) for the Schema Registry course
- **Single Broker:** All examples run on a single-broker cluster (`localhost:9092`)
- **Replication Factor:** Limited to 1 due to single broker
- **Transcript:** `commands.txt` and `commands.md` cover two practice arcs, 22 Jul–2 Aug 2026:
  - 211 prompts, Kafka Fundamentals
  - 94 prompts, Schema Registry & Avro
- **Screenshots:** two independent batches:
  - 196 shots, 22–29 Jul, exported to `kafka-beginners.pptx` and `kafka-beginners.pdf`
  - 109 shots, 30 Jul–2 Aug, exported to `kafka-schema-registry.pptx` and
    `kafka-schema-registry.pdf`, currently on disk in `images/`

---

## 🐛 Common Errors & Fixes

| Error                                              | Cause                                            | Fix                                                                     |
|----------------------------------------------------|--------------------------------------------------|-------------------------------------------------------------------------|
| `--bootstrap-server must be specified`             | Missing required flag                            | Add `--bootstrap-server localhost:9092` to every command                |
| `TopicExistsException`                             | Creating topic that already exists               | Omit `--create` or use `--if-not-exists`                                |
| `GroupIdNotFoundException`                         | Consumer group never had a member                | Run a consumer with `--group GROUP_NAME` once to create it              |
| `InvalidReplicationFactorException`                | RF > broker count                                | Use `--replication-factor 1` on single-broker clusters                  |
| `kafka.tools.DefaultMessageFormatter` not found    | Kafka 4.0 removed the class                      | Use `--property print.key=true --property print.partition=true` instead |
| `AvroTypeException: Expected field name not found` | JSON doesn't match the Avro schema's field names | Match the schema exactly, or bump the schema version                    |
| `docker -it exec <container> bash` fails           | Flags placed before the subcommand               | Use `docker exec -it <container> bash` — flags go after `exec`          |
| REST Proxy `Bind ... 8082 ... already allocated`   | `fast-data-dev` already publishes 8082 itself    | Don't run a second REST Proxy container against the same host port      |

Full list with more context: `commands.md` sections 13 and 20.

---

## 📄 License & Attribution

This repository documents practice with **Apache Kafka** (open source), Conduktor's
**Kafka for Beginners** course, and Confluent's **Schema Registry & Avro** course
materials.

**Course Instructor:** Stephane Maarek
**Course Resources:** https://links.datacumulus.com/kafka-beginners-coupon
**Discount Code:** JUL_26_GET_STARTED

---

**Created:** 22 Jul 2026
**Last Updated:** 2 Aug 2026
