# simple-rotating-file-writer

It provides a mechanism for writing log messages to a file, automatically rotating the log file and managing backup
files when a specified size limit is exceeded.

### Usage

```python
from rotating_file import SimpleRotatingFileWriter

with SimpleRotatingFileWriter('log.txt', max_bytes=50, backup_count=3) as writer:
    writer.write("1234567890123456789012345678901234567890")  # 40 chars
    writer.write("12345678901234567890")  # 20 chars, should trigger rollover

```

```python
writer = SimpleRotatingFileWriter('log.txt', max_bytes=50, backup_count=3)
writer.write("1234567890123456789012345678901234567890")
writer.write("12345678901234567890")
writer.close()

```
