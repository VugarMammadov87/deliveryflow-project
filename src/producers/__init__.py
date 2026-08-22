"""Synthetic logistics event producer package.

The package groups the generator code used by local demos, smoke tests, and
Docker Compose commands. Keeping it importable lets tests and scripts reuse the
same event builder instead of maintaining separate sample payloads.
"""
