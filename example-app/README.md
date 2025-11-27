# Example Application - DLQ Replay Testing

## ⚠️ CRITICAL: Infinite Loop Happening Right Now?

If you see logs like this repeating forever:

```
sending to dlq topic: test-dlq
Polled 1 records from DLQ
Committed offsets for batch
processing order: order-001
exception: order amount cannot be null or negative
sending to dlq topic: test-dlq
```

**You have a different problem!** The replay is working, but:

1. Replay sends messages from DLQ → `orders` topic
2. Orders consumer processes them → **FAILS** (bad data)
3. Failed messages sent BACK to DLQ
4. Replay picks them up again → **INFINITE CYCLE!**

### STOP IT NOW:

```bash
# 1. STOP your Spring Boot application (Ctrl+C)
# 2. Run the emergency stop script:
./emergency-stop.sh
# 3. Restart your application
```

**Then read "How to Avoid the Infinite Replay Cycle" below!**

---

## Current Situation

If you just installed the fixed library and are seeing 40,000+ messages being replayed, this is EXPECTED behavior! Here's why:

### What Happened

1. **The Original Bug** created ~40,000 duplicate messages in your `test-dlq` topic
2. Those messages are **real and stored on disk** in Kafka
3. **The Fix** prevents infinite looping, but it will still replay all existing messages once
4. You're seeing all 40,000+ messages being replayed (correctly, without looping)

### What You Need to Do

**Clean up the DLQ topic BEFORE testing:**

```bash
# Run the cleanup script
./cleanup-dlq.sh
```

This will delete and recreate the `test-dlq` topic, giving you a fresh start.

## Scripts Available

### 1. `cleanup-dlq.sh` - Clean Up DLQ Topic

Deletes and recreates the `test-dlq` topic to remove all duplicate messages.

```bash
./cleanup-dlq.sh
```

**Use this when:**
- You've just installed the fix and want a clean slate
- Your DLQ has accumulated too many duplicate messages
- You want to test the replay functionality from scratch

### 2. `check-dlq-status.sh` - Check DLQ Status

Shows you how many messages are in the DLQ and consumer group offsets.

```bash
./check-dlq-status.sh
```

**Use this to:**
- See how many messages are actually in the DLQ
- Check if the replay consumer group has committed offsets
- Verify cleanup was successful

### 3. `run-and-test.sh` - Run Example Application

Starts the example application.

```bash
./run-and-test.sh
```

## Testing the Fixed Replay Functionality

### After Cleanup

1. **Clean up first:**
   ```bash
   ./cleanup-dlq.sh
   ```

2. **Start the application:**
   ```bash
   ./run-and-test.sh
   ```

3. **Trigger some failures** to get messages in the DLQ:
   ```bash
   # Send a message that will fail and go to DLQ
   curl -X POST http://localhost:8080/your-endpoint-that-fails
   ```

4. **Check DLQ status:**
   ```bash
   ./check-dlq-status.sh
   ```

5. **Replay messages (first time - will replay all):**
   ```bash
   curl -X POST http://localhost:8080/dlq/replay/test-dlq
   ```

6. **Replay again (should replay 0 messages - already processed):**
   ```bash
   curl -X POST http://localhost:8080/dlq/replay/test-dlq
   ```
   
   You should see: `"Replay completed. Success: 0, Failures: 0"`

7. **Force replay (will replay all again - creates duplicates!):**
   ```bash
   curl -X POST "http://localhost:8080/dlq/replay/test-dlq?forceReplay=true"
   ```

## Expected Behavior

### ✅ Correct Behavior (After Fix)

```
First replay:
  Replayed 8 messages successfully
  
Second replay (same endpoint call):
  Replayed 0 messages successfully  ← Already processed!
  
Force replay:
  Replayed 8 messages successfully  ← Reprocessed all (creates duplicates)
```

### ❌ Old Broken Behavior (Before Fix)

```
Every replay call:
  Polled 1 records from DLQ
  Committed offsets for batch
  Polled 1 records from DLQ
  ... (repeats thousands of times)
  Replayed 10600 messages successfully  ← From only 8 messages!
```

## API Endpoints

### GET /dlq?topic=test-dlq
View all messages in the DLQ

```bash
curl http://localhost:8080/dlq?topic=test-dlq
```

### POST /dlq/replay/{topic}
Replay unprocessed messages (respects committed offsets, validates messages normally)

```bash
curl -X POST http://localhost:8080/dlq/replay/test-dlq
```

**Use this when:** You've fixed the underlying issue (code or data) and messages will now process successfully.

### POST /dlq/replay/{topic}?forceReplay=true
Force replay ALL messages from beginning (⚠️ creates duplicates!)

```bash
curl -X POST "http://localhost:8080/dlq/replay/test-dlq?forceReplay=true"
```

**Use this when:** You want to replay already-processed messages (testing only).

### POST /dlq/replay/{topic}?skipValidation=true
Replay messages and skip validation (⚠️ testing only!)

```bash
curl -X POST "http://localhost:8080/dlq/replay/test-dlq?skipValidation=true"
```

**Use this when:** You want to test the replay functionality with invalid data without fixing it first.

### POST /dlq/replay/{topic}?forceReplay=true&skipValidation=true
Force replay from beginning AND skip validation (⚠️ testing only!)

```bash
curl -X POST "http://localhost:8080/dlq/replay/test-dlq?forceReplay=true&skipValidation=true"
```

**Use this when:** You want to reprocess all messages including already-replayed ones, skipping validation.

## How to Test DLQ Replay (The Right Way!)

You're absolutely right - if messages are in the DLQ because they failed validation, replaying them will just cause them to fail again! Here's how to properly test:

### Solution 1: Use skipValidation Mode (For Testing Only!)

The replay endpoint now supports a `skipValidation` parameter that adds a special header to replayed messages, telling your consumer to skip validation:

```bash
# Replay messages and skip validation (for testing with bad data)
curl -X POST "http://localhost:8080/dlq/replay/test-dlq?skipValidation=true"
```

When `skipValidation=true`:
- ✅ Replayed messages get an `X-Replay-Mode` header
- ✅ Your consumer sees this header and skips validation
- ✅ Messages are "successfully processed" (logged but not validated)
- ✅ No infinite loop!

**⚠️ WARNING**: This is for testing/demo only! In production, you should fix the data or code before replaying.

### Solution 2: Fix the Data Before Replaying (Production Approach)

1. **View DLQ messages:**
   ```bash
   curl http://localhost:8080/dlq?topic=test-dlq
   ```

2. **Identify which ones can be fixed:**
   - order-001: amount = -100 → Can you fix the order in your database?
   - order-002: customerId = null → Can you look up the customer?
   - order-003: paymentMethod = null → Can you contact the customer?

3. **Fix the underlying data in your database**

4. **Then replay:**
   ```bash
   curl -X POST http://localhost:8080/dlq/replay/test-dlq
   ```

### Solution 3: Fix Your Code First (If It's a Code Bug)

If messages failed because of a bug in your processing logic:

1. **Fix the bug** in `OrderProcessingService`
2. **Rebuild and restart** the application
3. **Replay messages:**
   ```bash
   curl -X POST http://localhost:8080/dlq/replay/test-dlq
   ```
   
Now they'll process successfully!

### Solution 4: Don't Replay Unfixable Messages

Some messages are permanently invalid and should never be replayed:
- Negative amounts (data corruption)
- Missing required fields that can't be recovered

For these:
1. **Archive them** (query the DLQ API and save to a file)
2. **Clean up the DLQ:** `./cleanup-dlq.sh`
3. **Move on** - don't try to replay them

## How to Avoid the Infinite Replay Cycle

### The Problem

When you replay messages from the DLQ, they go back to the original topic. If those messages **still fail processing**, they'll be sent back to the DLQ, and replay will pick them up again!

```
DLQ → Replay → orders topic → Processing FAILS → DLQ → Replay → ... (INFINITE!)
```

### The Solution

**Option 1: Fix the Data Before Replaying**

Don't replay messages with unfixable errors! Examples of unfixable errors:
- ❌ `order-001`: amount = -100 (negative amount is always invalid)
- ❌ `order-002`: customerId = null (required field missing)
- ❌ `order-003`: paymentMethod = null (required field missing)

**Only replay messages that can succeed!**

**Option 2: Fix Your Code First**

If messages are failing due to a bug in your code:
1. Fix the bug in your application
2. Restart the application  
3. THEN call the replay endpoint

**Option 3: Mark Errors as Non-Retryable**

For data validation errors (like negative amounts), make sure they're configured as **non-retryable** so they don't get retried forever.

In your code:
```java
@CustomKafkaListener(
    topics = "orders",
    groupId = "orders-consumer",
    maxAttempts = 3,
    nonRetryableExceptions = {
        IllegalArgumentException.class,  // ✅ Don't retry validation errors
        ValidationException.class         // ✅ Don't retry validation errors
    }
)
```

**Option 4: Manually Clean DLQ (Remove Bad Messages)**

If you have specific bad messages in the DLQ that should never be replayed:
1. Use `GET /dlq?topic=test-dlq` to view them
2. Identify the unfixable ones
3. Clean up the DLQ: `./cleanup-dlq.sh`
4. Only send valid test data

## Troubleshooting

### Problem: Infinite loop - messages keep going DLQ → orders → DLQ

**Cause:** Your test messages have unfixable errors (negative amounts, null required fields, etc.)

**Solution:** 
1. STOP your application (Ctrl+C)
2. Run `./emergency-stop.sh`
3. Fix your test data or code
4. Restart and test with valid messages

### Problem: Still seeing 40,000+ messages being replayed

**Solution:** You haven't cleaned up the DLQ yet. Run `./cleanup-dlq.sh`

### Problem: Replay returns 0 messages but I know there are messages in DLQ

**Reason:** The replay consumer group has already processed those messages.

**Solution:** Either:
- Use `forceReplay=true` to replay anyway
- OR delete the consumer group: `kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group dlq-replay-test-dlq`

### Problem: How do I know if the fix is working?

**Test:**
1. Clean up DLQ: `./cleanup-dlq.sh`
2. Add a few test messages to DLQ
3. Call replay: `curl -X POST http://localhost:8080/dlq/replay/test-dlq`
4. Call replay AGAIN: `curl -X POST http://localhost:8080/dlq/replay/test-dlq`

**Expected:** Second call should replay 0 messages (already processed)

**If broken:** Second call would replay messages again (infinite loop)

