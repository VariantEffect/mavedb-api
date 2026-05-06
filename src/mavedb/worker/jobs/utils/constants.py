"""Constants used across worker jobs.

This module centralizes configuration constants used by various worker jobs
including queue names, timeouts, and retry limits. This provides a single
source of truth for job configuration values.
"""

### Mapping job constants
MAPPING_QUEUE_NAME = "vrs_mapping_queue"
MAPPING_CURRENT_ID_NAME = "vrs_mapping_current_job_id"
MAPPING_BACKOFF_IN_SECONDS = 15

### Linking job constants
LINKING_BACKOFF_IN_SECONDS = 15 * 60

### Backoff constants
ENQUEUE_BACKOFF_ATTEMPT_LIMIT = 5
