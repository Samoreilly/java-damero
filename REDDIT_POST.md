Hey everyone.


I am a 3rd year CS student and I have been diving deep into big data and performance optimization. I found myself replacing the same retry loops, dead letter queue managers, and circuit breakers for every single Kafka consumer I built, it got boring.


So I spent the last few months building a wrapper library to handle the heavy lifting.


It is called java-damero. The main idea is that you just annotate your listener and it handles retries, batch processing, deserialization, DLQ routing, and observability automatically.


I tried to make it technically robust under the hood:
- It supports Java 21 Virtual Threads to handle massive concurrency without blocking OS threads.

- I built a flexible deserializer that infers types from your method signature, so you can send raw JSON without headers.

- It has full OpenTelemetry tracing built in, so context propagates through all retries and DLQ hops.

- Batch processing mode that only commits offsets when the full batch works.

- I also allow you to plug in a Redis cache for distributed systems with a backoff to an in memory cache.


I benchmarked it on my laptop and it handles batches of 6000 messages with about 350ms latency. I also wired up a Redis-backed deduplication layer that fails over to local caching if Redis goes down.
Screenshots are in the /PerformanceScreenshots folder in the /src

<dependency>
    <groupId>io.github.samoreilly</groupId>
    <artifactId>java-damero</artifactId>
    <version>1.0.4</version>
</dependency>

https://central.sonatype.com/artifact/io.github.samoreilly/java-damero/overview


I would love if you guys could give feedback. I tried to keep the API clean so you do not need messy configuration beans just to get reliability.


Thanks for reading
https://github.com/Samoreilly/java-damero