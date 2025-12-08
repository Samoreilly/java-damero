# Batch Processing


### Refactor method to accept a List or overload method
# Let user define a batch size and maybe a lower bound (when the messages reach the lower bound, processing starts ??)
# List must be thread safe

```
Batch mangager per TOPIC with topic as key
Will use atomic long to keep track of the batch size
```