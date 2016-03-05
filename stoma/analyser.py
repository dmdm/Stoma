from .const import ITEM_STATE_ANALYSING, ITEM_STATE_NEED_INDEXING, ITEM_STATE_NEED_ANALYSIS
from .models import Item


class Analyser:

    def __init__(self, lgg, sess, tika):
        self.lgg = lgg
        self.sess = sess
        self.tika = tika

    def analyse(self, filter_crit=None):
        tika = self.tika
        lgg = self.lgg
        sess = self.sess
        fil = [
            Item.state == ITEM_STATE_NEED_ANALYSIS
        ]
        if filter_crit:
            fil += filter_crit

        # TODO Refactor handling of session and transaction to be suitable for parallel execution
        paths = [r.path for r in self.sess.query(Item.path).filter(*fil).order_by(Item.path)]
        for p in paths:
            lgg.debug("Analysing '{}'".format(p))

            it = sess.query(Item).with_for_update().get(p)
            it.state = ITEM_STATE_ANALYSING
            sess.flush()

            pym_meta = tika.pym(p)
            it.mime_type = pym_meta['mime_type']
            it.language = pym_meta['language']
            it.set_meta(pym_meta)
            it.state = ITEM_STATE_NEED_INDEXING
            sess.flush()
