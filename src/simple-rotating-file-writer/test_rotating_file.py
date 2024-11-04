import os
import shutil
import unittest

from rotating_file import SimpleRotatingFileWriter


class TestSimpleRotatingFileWriter(unittest.TestCase):
    def setUp(self):
        """Set up a temporary directory and file for testing."""
        self.test_dir = 'test_logs'
        os.makedirs(self.test_dir, exist_ok=True)
        self.filename = os.path.join(self.test_dir, 'logfile.log')
        self.writer = SimpleRotatingFileWriter(self.filename, max_bytes=50, backup_count=3)

    def tearDown(self):
        """Clean up the temporary directory after tests."""
        self.writer.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_initialization(self):
        """Test that the writer initializes correctly."""
        self.assertEqual(self.writer.base_filename, self.filename)
        self.assertEqual(self.writer.max_bytes, 50)
        self.assertEqual(self.writer.backup_count, 3)

    def test_write_message(self):
        """Test writing a message to the file."""
        with self.writer:
            self.writer.write("Test message 1")
            self.writer.write("Test message 2")

        with open(self.filename, 'r') as f:
            content = f.readlines()
            self.assertEqual(len(content), 2)
            self.assertIn("Test message 1\n", content)
            self.assertIn("Test message 2\n", content)

    def test_rollover_functionality(self):
        """Test that the rollover works when the max_bytes limit is exceeded."""
        with self.writer:
            self.writer.write("1234567890123456789012345678901234567890")  # 40 chars
            self.writer.write("12345678901234567890")  # 20 chars, should trigger rollover

        # Check the existence of backup files
        self.assertTrue(os.path.exists(self.writer.rotation_filename(os.path.join(self.test_dir, 'logfile.log.1'))))
        self.assertTrue(os.path.exists(self.writer.rotation_filename(os.path.join(self.test_dir, 'logfile.log'))))

    def test_context_manager(self):
        """Test that the context manager properly opens and closes the file."""
        with self.writer:
            self.writer.write("Within context")
            self.assertIsNotNone(self.writer.current_file)

        # After exiting the context, the file should be closed
        self.assertIsNone(self.writer.current_file)

    def test_max_backup_count(self):
        """Test that the maximum backup count is enforced."""
        with self.writer:
            for i in range(10):  # Write enough messages to trigger multiple rollovers
                self.writer.write(f"Log message {i}")

        # Check for the number of backup files
        backup_files = [f for f in os.listdir(self.test_dir) if f.startswith('logfile.log.')]
        self.assertEqual(len(backup_files), 3)  # Should only keep 3 backups


if __name__ == '__main__':
    unittest.main()
