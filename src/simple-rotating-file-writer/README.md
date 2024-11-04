# simple-rotating-file-writer

It provides a mechanism for writing log messages to a file, automatically rotating the log file and managing backup
files when a specified size limit is exceeded.

### Usage

**Basic write**

```python
from rotating_file import SimpleRotatingFileWriter

writer = SimpleRotatingFileWriter('log.txt', max_bytes=50, backup_count=3)
writer.write("1234567890123456789012345678901234567890")
writer.write("12345678901234567890")
writer.close()

```

This will create `log.txt` and rotate it to `log.txt.1` once the size limit is reached.

**Context manager**

```python
with SimpleRotatingFileWriter('log.txt', max_bytes=50, backup_count=3) as writer:
    writer.write("1234567890123456789012345678901234567890")  # 40 chars
    writer.write("12345678901234567890")  # 20 chars, should trigger rollover

```

**Custom filename rotation**

```python
from rotating_file import sequence_extension_namer

with SimpleRotatingFileWriter('log.txt', max_bytes=50, backup_count=3) as writer:
    writer.namer = sequence_extension_namer
    writer.write("1234567890123456789012345678901234567890")  # 40 chars
    writer.write("12345678901234567890")  # 20 chars, should trigger rollover

```

This will result in `log.txt` and `log.1.txt` being created, with the custom naming convention applied.
