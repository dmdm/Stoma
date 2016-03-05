import argparse
import datetime
import logging
import os
import sys
import time

import alembic.command
import alembic.config
import transaction
from zope.sqlalchemy import mark_changed

from ..elastics import ElasticSearchRestClient
from ..cli import Cli
from ..models import create_all, Item
from ..walker import Walker
from ..tika import TikaRestClient
from ..analyser import Analyser
from ..indexer import Indexer
from ..const import DEFAULT_INDEX


class Runner(Cli):
    def __init__(self):
        super().__init__()

    def init_app(self, args, lgg=None, rc=None, rc_key=None, setup_logging=True):
        super().init_app(args=args, lgg=lgg, rc=rc, rc_key=rc_key,
            setup_logging=setup_logging)

    def cmd_initdb(self):
        self.lgg.info('Initialising database')
        sess = self._sess
        with transaction.manager:
            self._create_schema(sess)
            mark_changed(sess)
        with transaction.manager:
            # Create all models
            create_all()

            if self.args.alembic_config:
                alembic_cfg = alembic.config.Config(self.args.alembic_config)
                alembic.command.stamp(alembic_cfg, "head")

            mark_changed(sess)

    @staticmethod
    def _create_schema(sess):
        sess.execute('CREATE SCHEMA IF NOT EXISTS stoma')

    def cmd_index(self):
        self.lgg.info('Indexing')
        __ = logging.getLogger('requests.packages.urllib3.connectionpool')
        __.setLevel(logging.WARN)

        tika = TikaRestClient()
        if not tika.is_running():
            raise Exception('Tika server is not running')
        self.lgg.debug(tika.version())
        ela = ElasticSearchRestClient(lgg=self.lgg)
        if not ela.is_running():
            raise Exception('ElasticSearch server is not running')
        self.lgg.debug(ela.version())
        self.lgg.debug(ela.count())

        w = Walker(lgg=self.lgg, sess=self.sess)
        ana = Analyser(lgg=self.lgg, sess=self.sess, tika=tika)
        ixr = Indexer(lgg=self.lgg, sess=self.sess, ela=ela)

        transaction.begin()
        try:
            w.walk(self.args.start_dir)
            transaction.commit()
        except Exception:
            transaction.abort()
            self.lgg.error('Transaction aborted')
            raise

        transaction.begin()
        try:
            ana.analyse()
            transaction.commit()
        except Exception:
            transaction.abort()
            self.lgg.error('Transaction aborted')
            raise

        transaction.begin()
        try:
            ixr.index()
            transaction.commit()
        except Exception:
            transaction.abort()
            self.lgg.error('Transaction aborted')
            raise

    def cmd_drop(self):
        self.lgg.info('Dropping index and database cache')
        __ = logging.getLogger('requests.packages.urllib3.connectionpool')
        __.setLevel(logging.WARN)

        ela = ElasticSearchRestClient(lgg=self.lgg)
        if not ela.is_running():
            raise Exception('ElasticSearch server is not running')
        self.lgg.debug(ela.version())
        self.lgg.debug(ela.count())

        transaction.begin()
        try:
            self.sess.query(Item).delete()
            transaction.commit()
        except Exception:
            transaction.abort()
            self.lgg.error('Transaction aborted')
            raise
        else:
            ela.delete_index(DEFAULT_INDEX)


def parse_args(runner, argv):
    # Main parser
    p = argparse.ArgumentParser()
    runner.parser = p
    runner.add_parser_args(p, (('config', True), ('format', False),
        ('locale', False), ('verbose', False), ('alembic-config', False)))

    sp = p.add_subparsers(
        title="Commands",
        dest="subparser_name",
        help="""Type 'validate_emails COMMAND --help'"""
    )

    p_initdb = sp.add_parser(
        'initdb',
        parents=[],
        help="Initalise database",
        add_help=True
    )
    p_initdb.set_defaults(func=runner.cmd_initdb)

    p_index = sp.add_parser(
        'index',
        parents=[],
        help="Index filesystem tree",
        add_help=True
    )
    p_index.set_defaults(func=runner.cmd_index)
    p_index.add_argument(
        'start_dir',
        help="""Path to start directory."""
    )

    p_drop = sp.add_parser(
        'drop',
        parents=[],
        help="D rop index and database cache",
        add_help=True
    )
    p_drop.set_defaults(func=runner.cmd_drop)

    return p.parse_args(argv[1:])


def main(argv=None):
    start_time = time.time()
    if not argv:
        argv = sys.argv

    app_name = os.path.basename(argv[0])
    lgg = logging.getLogger('cli.' + app_name)

    try:
        runner = Runner()
        args = parse_args(runner, argv)

        runner.init_app(args, lgg=lgg, setup_logging=True)
        if hasattr(args, 'func'):
            args.func()
        else:
            runner.run()
    except Exception as exc:
        lgg.exception(exc)
        lgg.fatal('Program aborted!')
    else:
        lgg.info('Finished.')
    finally:
        lgg.info('Time taken: {}'.format(
            datetime.timedelta(seconds=time.time() - start_time))
        )
