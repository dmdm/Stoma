import functools
import os

from .const import (ITEM_STATE_NEED_INDEXING, ITEM_STATE_NEED_DELETION,
    ITEM_STATE_INDEXING, ITEM_STATE_INDEXED, ITEM_STATE_DELETED,
    DEFAULT_DOC_TYPE, DEFAULT_INDEX)
from .models import Item


class Indexer:

    def __init__(self, lgg, sess, ela, index=DEFAULT_INDEX,
            doc_type=DEFAULT_DOC_TYPE):
        self.lgg = lgg
        self.sess = sess
        self.ela = ela
        self.index_name = index
        self.doc_type = doc_type

    def index(self, filter_crit=None):
        if not self.ela.is_running():
            raise Exception('ElasticSearch server is not running')
        self._save(filter_crit)
        self._delete(filter_crit)

    def _save(self, filter_crit):
        lgg = self.lgg
        sess = self.sess
        save = functools.partial(self.ela.save, index=self.index_name,
            doc_type=self.doc_type)
        fil = [
            Item.state == ITEM_STATE_NEED_INDEXING
        ]
        if filter_crit:
            fil += filter_crit
        rs = sess.query(Item).filter(*fil).with_for_update()
        for it in rs:
            lgg.debug('Indexing {}'.format(it.path))
            it.state = ITEM_STATE_INDEXING
            sess.flush()

            data = {
                'path': it.path,
                'tags': it.path.split(os.path.sep),
                'mime_type': it.mime_type,
                'encoding': it.encoding,
                'language': it.language,
                'size': it.size,
                'ctime': it.item_ctime,
                'mtime': it.item_mtime,
                'meta': it.meta_json,
                'text': it.data_text
            }
            if it.meta_json and 'language' in it.meta_json:
                data['language'] = it.meta_json['language']
            id_ = it.ela_id if it.ela_id else None
            r = save(id_=id_, data=data)
            if not it.ela_id:
                it.ela_id = r['_id']
            it.ela_version = r['_version']

            it.state = ITEM_STATE_INDEXED
            sess.flush()

    def _delete(self, filter_crit):
        lgg = self.lgg
        sess = self.sess
        delete = functools.partial(self.ela.delete, index=self.index_name,
            doc_type=self.doc_type)
        fil = [
            Item.state == ITEM_STATE_NEED_DELETION
        ]
        if filter_crit:
            fil += filter_crit
        rs = sess.query(Item).filter(*fil).with_for_update()
        for it in rs:
            lgg.debug('Deleting from index {}'.format(it.path))
            it.state = ITEM_STATE_INDEXING
            sess.flush()

            id_ = it.ela_id
            ok = delete(id_=id_)
            if not ok:
                self.lgg.warn('Item not deleted: {}, '.format(id_, it.path))
            it.ela_id = None
            it.ela_version = None

            it.state = ITEM_STATE_DELETED
            sess.flush()
