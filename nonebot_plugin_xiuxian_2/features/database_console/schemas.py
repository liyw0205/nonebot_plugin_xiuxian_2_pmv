"""Durable bounds of the database console.

The console is a guarded operator tool: it may read and edit any table the
injected catalog provider lists, so the contract is the set of limits and
literals that a caller is allowed to influence.  ``LIKE_SEARCH_OPERATOR`` and
``RANGE_SEARCH_OPERATORS`` are the complete whitelist for ``search_condition``,
the single user value whose operator half reaches a statement as text; every
value beside it is bound as a parameter.  ``MAX_TABLE_PAGE_SIZE`` caps how wide one request may read and
``MAX_RANGE_SEARCH_VALUES`` caps a numeric range.  The primary-key names encode
how a row id string is split back apart again, including the composite
``impart_cards`` case.
"""

from __future__ import annotations


MIN_TABLE_PAGE_SIZE = 1
DEFAULT_TABLE_PAGE_SIZE = 10
MAX_TABLE_PAGE_SIZE = 200
LIKE_SEARCH_OPERATOR = "="
RANGE_SEARCH_OPERATORS = (">", "<")
MAX_RANGE_SEARCH_VALUES = 2
BATCH_SET_OPERATION = "set"
BATCH_ADD_OPERATION = "add"
BATCH_SUBTRACT_OPERATION = "subtract"
BATCH_EDIT_OPERATIONS = (
    BATCH_SET_OPERATION,
    BATCH_ADD_OPERATION,
    BATCH_SUBTRACT_OPERATION,
)
DEFAULT_PRIMARY_KEY = "id"
DYNAMIC_TABLE_PRIMARY_KEY = "user_id"
IMPART_CARDS_TABLE = "impart_cards"
IMPART_CARDS_PRIMARY_KEYS = ("user_id", "card_name")

__all__ = [
    "BATCH_ADD_OPERATION",
    "BATCH_EDIT_OPERATIONS",
    "BATCH_SET_OPERATION",
    "BATCH_SUBTRACT_OPERATION",
    "DEFAULT_PRIMARY_KEY",
    "DEFAULT_TABLE_PAGE_SIZE",
    "DYNAMIC_TABLE_PRIMARY_KEY",
    "IMPART_CARDS_PRIMARY_KEYS",
    "IMPART_CARDS_TABLE",
    "LIKE_SEARCH_OPERATOR",
    "MAX_RANGE_SEARCH_VALUES",
    "MAX_TABLE_PAGE_SIZE",
    "MIN_TABLE_PAGE_SIZE",
    "RANGE_SEARCH_OPERATORS",
]
