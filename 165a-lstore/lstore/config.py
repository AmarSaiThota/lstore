# ──────────────────────────────────────────────
# config.py  —  Single source of truth for L-Store constants
# Every module should:  from lstore.config import *
# ──────────────────────────────────────────────

# Metadata column indices (order in every base/tail page set)
INDIRECTION_COLUMN = 0
RID_COLUMN         = 1
TIMESTAMP_COLUMN   = 2
SCHEMA_COLUMN      = 3
TPS_COLUMN         = 4       # Tail Page Sequence — last merged tail RID
BASE_RID_COLUMN    = 5       # For tail records: owning base RID; for base: self

NUM_META_COLUMNS   = 6

# Page constants
PAGE_SIZE          = 4096
RECORDS_PER_PAGE   = 512     # PAGE_SIZE / 8  (64-bit integers)
MAX_BASE_PAGES_PER_RANGE = 16

# Null sentinel  (used throughout to mean "no value" / "not updated")
NULL_VALUE = -(2**63)
