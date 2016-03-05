import configparser
import logging
import logging.config
import os
import sys

import pym.auth.models
import pym.cache.configure
import pym.cli
import pym.exc
import pym.lib
import pym.testing
from pym.rc import Rc
from .models import DbSession, init


mlgg = logging.getLogger(__name__)


class DummyArgs(object):
    pass


class Cli(pym.cli.Cli):

    def __init__(self):
        super().__init__()

    def base_init(self, args, lgg=None, rc=None, rc_key=None, setup_logging=True):
        """
        Initialises base for CLI apps: logger, console, rc, Pyramid Configurator

        Used by :meth:`init_app` and :meth:`init_web_app`.

        :param args: Namespace of parsed CLI arguments
        :param lgg: Inject a logger, or keep the default module logger
        :param rc: Inject a RC instance, or keep the loaded one.
        :param rc_key: *obsolete*
        :param setup_logging: Whether or not to setup logging as configured in
            rc. Default is True.
        """
        self.args = args
        fn_config = os.path.abspath(args.config)
        self.rc_key = rc_key
        if setup_logging:
            logging.config.fileConfig(
                fn_config,
                dict(
                    __file__=fn_config,
                    here=os.path.dirname(fn_config)
                ),
                # Keep module loggers
                disable_existing_loggers=False
            )
        if lgg:
            self.lgg = lgg

        if hasattr(args, 'verbose'):
            if args.verbose > 1:
                lgg.setLevel(logging.DEBUG)
            elif args.verbose > 0:
                lgg.setLevel(logging.INFO)

        p = configparser.ConfigParser()
        p.read(fn_config)
        settings = dict(p['app:main'])
        if 'environment' not in settings:
            raise KeyError('Missing key "environment" in config. Specify '
                'environment in INI file "{}".'.format(args.config))

        self.lang_code, self.encoding = pym.cli.init_cli_locale(
            args.locale if hasattr(args, 'locale') else None,
            detach_stdout=settings['environment'] != 'testing'
        )
        self.lgg.debug("TTY? {}".format(sys.stdout.isatty()))
        self.lgg.debug("Locale? {}, {}".format(self.lang_code, self.encoding))

        if not rc:
            if not args.etc_dir:
                args.etc_dir = os.path.join(args.root_dir, 'etc')
            rc = Rc(
                environment=settings['environment'],
                root_dir=args.root_dir,
                etc_dir=args.etc_dir
            )
            rc.load()
            rc.s('environment', settings['environment'])
        self.rc = rc

    def base_init2(self):
        self._sess = DbSession()

    def init_app(self, args, lgg=None, rc=None, rc_key=None, setup_logging=True):
        """
        Initialises Pyramid application for command-line use.

        Additional to :meth:`base_init`, initialises SQLAlchemy and a DB
        session, authentication module and the cache.

        :param args: Namespace of parsed CLI arguments
        :param lgg: Inject a logger, or keep the default module logger
        :param rc: Inject a RC instance, or keep the loaded one.
        :param rc_key: *obsolete*
        :param setup_logging: Whether or not to setup logging as configured in
            rc. Default is True.
        """
        self.base_init(args, lgg=lgg, rc=rc, rc_key=rc_key,
            setup_logging=setup_logging)

        init(self.rc.get_these('db.pym.sa.'))

        self.base_init2()
