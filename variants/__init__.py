"""Deliberately unsafe or versioned interpreters. Used only by tests to reproduce NDEs and their fixes.

Every class registers under the same workflow type name as the safe interpreter, so each one can
replay histories produced by any other.
"""
