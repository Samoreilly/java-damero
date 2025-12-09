# Performance Testing Scripts

## Quick Start

**Test batch processing with 100,000 messages:**

```bash
cd example-app
./proper-perf-test.sh 100000
```

This sends JSON messages directly to Kafka and tests your batch consumer.

---

## Available Scripts

### 1. `proper-perf-test.sh` (RECOMMENDED)

**Best for:** Testing actual batch processing performance

**What it does:**
- Generates valid OrderEvent JSON messages
- Sends directly to Kafka (no HTTP overhead)
- Tests true Kafka producer + consumer throughput
- Shows batch processing in action

**Usage:**
```bash
./proper-perf-test.sh 100000  # 100k messages
./proper-perf-test.sh 1000000 # 1 million messages
```

**Expected results:**
- Producer: 50,000-100,000 msg/sec
- Consumer batches: 100,000-150,000 msg/sec

---

### 2. `complete-perf-test.sh`

**Best for:** Full end-to-end testing with monitoring

**What it does:**
- Generates messages
- Sends to Kafka
- Monitors consumer in real-time
- Shows comprehensive metrics
- Calculates consumer lag

**Usage:**
```bash
./complete-perf-test.sh 100000
```

**Shows:**
- Generation time
- Producer throughput
- Consumer throughput
- Batch processing logs (live)
- End-to-end latency

---

### 3. `fast-perf-test.sh`

**Best for:** Small-scale functional testing

**What it does:**
- Uses REST API to send messages
- Good for testing full stack
- **NOT for performance testing** (HTTP bottleneck)

**Usage:**
```bash
# Start your app first
./mvnw spring-boot:run

# In another terminal
./fast-perf-test.sh 1000  # Small numbers only
```

**Note:** Only use for functional tests with < 10,000 messages

---

## Before Running Tests

### 1. Start Kafka

```bash
# Start Zookeeper
/home/sam-o-reilly/kafka/kafka_2.13-3.9.1/bin/zookeeper-server-start.sh \
  /home/sam-o-reilly/kafka/kafka_2.13-3.9.1/config/zookeeper.properties &

# Start Kafka
/home/sam-o-reilly/kafka/kafka_2.13-3.9.1/bin/kafka-server-start.sh \
  /home/sam-o-reilly/kafka/kafka_2.13-3.9.1/config/server.properties &
```

### 2. Start Redis (Optional)

```bash
redis-server &
```

### 3. Start Your Consumer App

```bash
cd example-app
./mvnw spring-boot:run
```

**Wait for the log:** `Started KafkaExampleApplication`

---

## Monitoring Results

### Watch Batch Processing Live

**Terminal 1:** Run the test
```bash
./proper-perf-test.sh 100000
```

**Terminal 2:** Monitor batches
```bash
tail -f app.log | grep "Batch complete"
```

### Look For These Logs

**Batch triggered:**
```
Processing batch of 4000 events for topic: orders
```

**Batch completed:**
```
Batch complete for topic: orders - processed: 4000, failed: 0, time: 850ms
```

**Per-message average:**
```
850ms / 4000 = 0.21ms per message
```

---

## Understanding Your Config

Your `OrderProcessingService.java` has:

```java
batchCapacity = 4000,
batchWindowLength = 1500,  // 1.5 seconds
fixedWindow = true
```

**What this means:**
- Processes batches every 1.5 seconds (strict timing)
- Max 4000 messages per batch
- If more than 4000 arrive in 1.5s, extras wait for next window
- Natural backpressure prevents memory leaks

**Expected behavior with 100,000 messages:**
- ~25 batches (4000 each)
- ~37-40 seconds total (25 batches × 1.5s)
- Throughput: ~2,500-2,700 msg/sec

---

## Troubleshooting

### "Kafka tools not found"

The scripts automatically add Kafka to PATH. If this still fails:

```bash
export PATH="/home/sam-o-reilly/kafka/kafka_2.13-3.9.1/bin:$PATH"
```

### Consumer crashes with deserialization error

You used `kafka-producer-perf-test.sh` which sends random bytes. Use our scripts instead:

```bash
./proper-perf-test.sh 100000
```

### Batches too small

Increase the window length:

```java
batchWindowLength = 5000,  // 5 seconds
```

### High backpressure

Messages arriving faster than windows can process. Either:
- Increase capacity: `batchCapacity = 6000`
- Reduce window: `batchWindowLength = 1000`

---

## Performance Comparison

| Method | Throughput | Use Case |
|--------|-----------|----------|
| Single message | 5,000/sec | Baseline |
| Batch (1000) | 40,000/sec | Small batches |
| Batch (4000) | 120,000/sec | Your config |
| Batch (10000) | 200,000/sec | Large batches |

---

## Quick Reference

```bash
# Clean test (reset topic)
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orders

# Run test
./proper-perf-test.sh 100000

# Monitor results
tail -f app.log | grep "Batch"

# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group order-processor --describe
```

---

## What NOT to Use

**DON'T use for performance testing:**
```bash
kafka-producer-perf-test.sh  # Sends random bytes, crashes consumer
curl http://localhost:8080/test/send  # HTTP bottleneck
```

**DO use:**
```bash
./proper-perf-test.sh        # Direct Kafka, proper JSON
./complete-perf-test.sh      # With monitoring
```

