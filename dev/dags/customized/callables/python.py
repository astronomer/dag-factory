"""
failure.py

Create a callable that intentionally "fails".

Author: Jake Roach
Date: 2024-10-22
"""


def succeeding_task():
    print("Task has executed successfully!")


def failing_task():
    raise Exception("Intentionally failing this Task to trigger on_failure_callback.")
