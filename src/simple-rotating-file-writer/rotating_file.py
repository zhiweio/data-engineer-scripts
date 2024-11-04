import os


class SimpleRotatingFileWriter:
    namer = None
    rotator = None

    def __init__(self, filename, max_bytes=0, backup_count=0):
        """
        Initialize SimpleRotatingFileWriter.

        :param filename: The base filename.
        :param max_bytes: Maximum bytes of the file; rotation occurs when exceeded.
        :param backup_count: Number of backup files; old files beyond this count will be deleted.
        """
        self.base_filename = filename
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.current_file = None
        self.current_file_size = 0

    def _open_file(self):
        """Open the file and initialize its size."""
        self.current_file = open(self.base_filename, "a")
        self.current_file_size = (
            os.path.getsize(self.base_filename)
            if os.path.exists(self.base_filename)
            else 0
        )

    def rotation_filename(self, default_name):
        """
        Modify the filename of a log file during rotation.

        :param default_name: The default name for the log file.
        :return: Modified filename.
        """
        if not callable(self.namer):
            result = default_name
        else:
            result = self.namer(default_name)
        return result

    def rotate(self, source, dest):
        """
        Rotate the current log file.

        :param source: The source filename, typically the base filename.
        :param dest: The destination filename, typically the rotated filename.
        """
        if not callable(self.rotator):
            if os.path.exists(source):
                os.rename(source, dest)
        else:
            self.rotator(source, dest)

    def do_rollover(self):
        """Perform the rollover of the file."""
        if self.backup_count > 0:
            # Rename backup files from the highest to lowest
            for i in range(self.backup_count - 1, 0, -1):
                sfn = self.rotation_filename(f"{self.base_filename}.{i}")
                dfn = self.rotation_filename(f"{self.base_filename}.{i + 1}")
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)

            # Rename current file to .1
            dfn = self.rotation_filename(f"{self.base_filename}.1")
            if os.path.exists(dfn):
                os.remove(dfn)
            # Close the file here to prevent renaming an unclosed file
            self.close()
            self.rotate(self.base_filename, dfn)

        # Reopen the base file
        self._open_file()  # Re-initialize current file and size

    def should_rollover(self, message):
        """Determine if rollover is needed based on message size."""
        return (
            self.max_bytes > 0
            and self.current_file_size + len(message) > self.max_bytes
        )

    def write(self, message):
        """
        Write a message to the file and rotate if necessary.

        :param message: The message to be written.
        """
        if self.current_file is None:
            self._open_file()

        # Check if rollover is necessary
        if self.should_rollover(message):
            self.do_rollover()

        # Write message and update file size
        self.current_file.write(message + "\n")
        self.current_file.flush()  # Ensure the message is written to the file
        self.current_file_size += (
            len(message) + 1
        )  # Update current file size (+1 for newline)

    def close(self):
        """Close the current file."""
        if self.current_file:
            self.current_file.close()
            self.current_file = None

    def __enter__(self):
        self._open_file()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
