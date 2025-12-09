# Batch Processing


Refactor method to accept a List or overload method
===
Let user define a batch size and maybe a lower bound (when the messages reach the lower bound, processing starts ??)
List must be thread safe
===


## Two different types of batching:
Fixed window and time-based

### Fixed window:
When a batch is done processing, the next batch must start processing after the fixed time interval.
Another batch cannot run in a batches processing window.

### Time-based:
A batch can immediately start processing after previous batch finishes processing.
The time interval is only there as a backoff mechanism.
For example. if batch doesnt reach capacity by the end of the interval, the batch will be executed immediately.

# Important things to know
Messages are only acknowledged after the batch is done processing, not when the batch enters queue.
This is done incase of failure, so kafka can continue from where it left off.

## Recommendations 
Use fixed window batching
Also enable deduplication



