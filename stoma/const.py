MIME_TYPE_DIRECTORY = 'inode/directory'
MIME_TYPE_JSON = 'application/json'
MIME_TYPE_DEFAULT = 'application/octet-stream'
MIME_TYPE_UNKNOWN = 'application/x-unknown'

TIKA_CMD = 'tika'

ITEM_STATE_UNCHANGED = 'unchanged'
"""Item in filesystem has not changed"""
ITEM_STATE_NEED_ANALYSIS = 'need_analysis'
"""
New in filesystem is new or changed, so is our database record.
Analyse this file.
"""
ITEM_STATE_NEED_DELETION = 'need_deletion'
"""
Item was deleted from filesystem.
Delete this file from ES too.
"""
ITEM_STATE_DELETED = 'deleted'
"""Item deleted from filesystem; set state in database and remove from ES"""
ITEM_STATE_ANALYSING = 'analysing'
"""Item in database is currently being analysed"""
ITEM_STATE_NEED_INDEXING = 'need_indexing'
"""Item in database is analysed, waiting for indexing"""
ITEM_STATE_INDEXING = 'indexing'
"""Item in database is currently being indexed"""
ITEM_STATE_INDEXED = 'indexed'
"""Item is indexed"""

IN_PROCESS_ITEM_STATES = (ITEM_STATE_ANALYSING, ITEM_STATE_NEED_INDEXING, ITEM_STATE_INDEXING)

STAT_ATTR = 'st_mode st_ino st_dev st_nlink st_uid st_gid st_size st_atime st_mtime st_ctime'.split(' ')

DEFAULT_INDEX = 'files'
DEFAULT_DOC_TYPE = 'file'
