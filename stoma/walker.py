import os
from datetime import datetime

import sqlalchemy as sa
from zope.sqlalchemy import mark_changed

from .const import (ITEM_STATE_NEED_ANALYSIS, ITEM_STATE_NEED_DELETION,
    ITEM_STATE_UNCHANGED, IN_PROCESS_ITEM_STATES, STAT_ATTR)
from .mime import guess_mime_type
from .models import Item, exclude_filter


ACTION_INSERT = 'i'
ACTION_UPDATE = 'u'
ACTION_DELETE = 'd'
ACTION_NOOP = 'n'


class Walker:

    def __init__(self, lgg, sess):
        self.lgg = lgg
        self.sess = sess
        self.items = {}
        self.known_items = {}
        self.start_dir = None

    def walk(self, start_dir):
        self.start_dir = os.path.abspath(start_dir)
        self.collect_items()
        self.load_items()
        self.compare()
        self.save_items()

    def collect_items(self):
        """
        Collects items from filesystem, starting with ``self.start_dir``.
        """
        self.lgg.debug("Collecting '{}'...".format(self.start_dir))
        items = {}
        for root, dirs, files in os.walk(self.start_dir):
            for f in files:
                fn = os.path.join(root, f)
                st = os.stat(fn, follow_symlinks=False)
                items[fn] = {'os_stat': st}
                items[fn]['item_ctime'] = datetime.fromtimestamp(st.st_ctime)
                items[fn]['item_mtime'] = datetime.fromtimestamp(st.st_mtime)
        self.items = items
        self.lgg.info('Collected {} items'.format(len(items)))

    def load_items(self):
        """
        Loads items from database, starting with ``self.start_dir``.
        """
        self.lgg.debug("Loading known items")
        fil = [Item.path.like(self.start_dir + '%')]
        rs = self.sess.query(
            Item.path, Item.item_mtime, Item.state
        ).filter(*fil)
        self.known_items = {r.path:
            {'item_mtime': r.item_mtime, 'state': r.state} for r in rs}
        self.lgg.info('Loaded {} known items'.format(len(self.known_items)))

    def compare(self):
        """
        Determines which items to create, update, and delete.

        Sets key 'action' on items to tell whether this item is to create or
        update in the database.
        """
        self.lgg.debug("Comparing")
        items = self.items
        known_items = self.known_items
        n_new = n_update = n_delete = n_unchanged = 0
        for it in items.keys():
            if it in known_items:
                if known_items[it]['state'] not in IN_PROCESS_ITEM_STATES:
                    if items[it]['item_mtime'] != known_items[it]['item_mtime']:
                        items[it]['mime_enc'] = guess_mime_type(it)
                        items[it]['action'] = ACTION_UPDATE
                        n_update += 1
                    else:
                        items[it]['action'] = ACTION_NOOP
                        n_unchanged += 1
                else:
                    items[it]['action'] = ACTION_NOOP
                    n_unchanged += 1
            else:
                items[it]['mime_enc'] = guess_mime_type(it)
                items[it]['action'] = ACTION_INSERT
                n_new += 1
        for it in known_items.keys():
            if it not in items:
                known_items[it]['action'] = ACTION_DELETE
                n_delete += 1
        self.lgg.info('{} new, {} update, {} delete, {} unchanged; sum: {}'.format(
            n_new, n_update, n_delete, n_unchanged, n_new + n_update + n_delete + n_unchanged)
        )

    def save_items(self):
        self.lgg.info("Saving...")
        sess = self.sess
        items = self.items
        known_items = self.known_items
        t = Item.__table__

        # 1. Assume all items are unchanged
        fil = exclude_filter()
        fil.append(t.c.path.like(self.start_dir + '%'))
        sess.execute(
            t.update().where(sa.and_(*fil)),
            {'state': ITEM_STATE_UNCHANGED}
        )
        # 2. Prepare
        updates = []
        inserts = []
        for p, d in items.items():
            if d['action'] == ACTION_UPDATE:
                updates.append({
                    'p': p,
                    'state': ITEM_STATE_NEED_ANALYSIS,
                    'mime_type': d['mime_enc'][0],
                    'encoding': d['mime_enc'][1],
                    'item_ctime': d['item_ctime'],
                    'item_mtime': d['item_mtime'],
                    'size': d['os_stat'].st_size,
                    'os_stat': {a: getattr(d['os_stat'], a) for a in STAT_ATTR},
                })
            elif d['action'] == ACTION_INSERT:
                inserts.append({
                    'path': p,
                    'state': ITEM_STATE_NEED_ANALYSIS,
                    'mime_type': d['mime_enc'][0],
                    'encoding': d['mime_enc'][1],
                    'item_ctime': d['item_ctime'],
                    'item_mtime': d['item_mtime'],
                    'size': d['os_stat'].st_size,
                    'os_stat': {a: getattr(d['os_stat'], a) for a in STAT_ATTR},
                })
        deletes = [k for k, v in known_items.items() if v is None]
        # 3. Update
        if updates:
            self.lgg.debug("Updating")
            upd = t.update().where(t.c.path == sa.bindparam('p'))
            sess.execute(upd, updates)
        # 4. inserts
        if inserts:
            self.lgg.debug("Inserting")
            ins = t.insert()
            sess.execute(ins, inserts)
        # 5. deletes
        if deletes:
            self.lgg.debug("Deleting")
            fil = exclude_filter()
            fil.append(t.c.path.in_(deletes))
            upd = t.update().where(sa.and_(*fil))
            sess.execute(upd, {'state': ITEM_STATE_NEED_DELETION})
        # 6. Flush
        mark_changed(sess)
