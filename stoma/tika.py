import io
import logging
import socket
import subprocess

import requests
from lxml import html

mlgg = logging.getLogger(__name__)


# See also https://github.com/chrismattmann/tika-python/blob/master/tika/tika.py
class TikaPymMixin:

    def pym(self, fn, hh=None):
        """
        Fetches a bundle of meta information about given file.

        Returned dict has these keys: ``content-type``, ``meta_json``,
        ``meta_xmp``, ``data_text``, ``data_html_head``, ``data_html_body``.

        Some keys such as ``meta_json`` may already contain a key
        ``content-type``. Still, we provide the top-level key ``content-type``,
         which is more accurate (and may differ from the others).

        :param fn: Filename.
        :param hh: Optional array with header fields for Tika server
        :return: Dict with meta info.
        """
        if hh is None:
            hh = {}
        m = {}

        ct = self.detect(fn, hh=hh)
        hh['content-type'] = ct

        s = self.language(fn, hh=hh)
        m['language'] = s if s else None

        s = self.meta(fn, 'json', hh=hh)
        m['meta_json'] = s if s else None

        s = self.meta(fn, 'xmp', hh=hh)
        m['meta_xmp'] = s if s else None

        s = self.tika(fn, 'html', hh=hh)
        if s:
            # Split head and body
            root = html.fromstring(s)
            # Our XML always has UTF-8
            m['data_html_head'] = html.tostring(root.head).decode('utf-8')
            m['data_html_body'] = html.tostring(root.body).decode('utf-8')
        else:
            m['data_html_head'] = None
            m['data_html_body'] = None

        s = self.tika(fn, 'text', hh=hh)
        m['data_text'] = s if s else None

        m['mime_type'] = ct
        return m


class TikaCli(TikaPymMixin):

    def __init__(self, tika_cmd='tika', encoding='utf-8'):
        """
        Communicate with TIKA via command-line.

        :param tika_cmd: Command to start the TIKA app. By default we assume
            you have created a shell wrapper named ``tika`` to start
            the appropriate jar. It must be in the path, e.g. in
            ``/usr/local/bin`` or ``~/bin``.

            E.g.:: bash

                #!/bin/sh

                java -jar /opt/tika/tika-app-1.7.jar "$@"

        :param encoding: Default UTF-8. Tells TIKA how to encode its output.
            Output read from the console is then decoded using this setting.
            Should match the encoding of the console (STDOUT).
        """
        self.tika_cmd = tika_cmd
        self.encoding = encoding

    def detect(self, fn):
        """
        Returns content-type.

        :param fn: Filename.
        :returns: The content-type.
        :rtype: string
        """
        switches = ['--detect']
        return self._run_cmd(fn, switches, decode=True)

    def rmeta(self, fn):
        """
        Returns recursive meta info about compound document.

        :param fn: Filename.
        :returns: List of dicts. Each dict has meta info about one of the
            compound documents. Key ``X-TIKA:content`` contains text document.
        :rtype: List of dicts
        """
        switches = ['--metadata', '--jsonRecursive']
        return self._run_cmd(fn, switches, decode=True)

    def unpack(self, fn, all_=False):
        raise NotImplementedError('Unpack not implemented for CLI')

    def meta(self, fn, type_='json'):
        """
        Returns meta info.
        :param fn: Filename.
        :param type_: 'json' or 'xmp'
        :return:
        """
        switches = ['--metadata']
        if type_ == 'xmp':
            switches.append('--xmp')
        else:
            switches.append('--json')
        return self._run_cmd(fn, switches, decode=True)

    def tika(self, fn, type_='text'):
        """
        Returns text or HTML of content.

        :param fn: Filename.
        :param type_: 'text', 'html'
        :return: HTML or text
        """
        switches = []
        if type_ == 'html':
            switches.append('--html')
        else:
            switches.append('--text')
        return self._run_cmd(fn, switches, decode=True)

    def _run_cmd(self, fn, switches, decode=True):
        a = [self.tika_cmd, '--encoding={}'.format(self.encoding)] + switches + [fn]
        try:
            s = subprocess.check_output(a, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exc:
            mlgg.error(exc.output.decode(self.encoding))
            raise
        else:
            s = s.strip()
            return s.decode(self.encoding) if decode else s


class TikaRestClient(TikaPymMixin):

    TYPE_MAP = {
        'text': {'accept': 'text/plain'},
        'html': {'accept': 'text/html'},
        'json': {'accept': 'application/json'},
        'xmp': {'accept': 'application/rdf+xml'},
        'csv': {'accept': 'text/csv'},
    }

    def __init__(self, host='localhost', port=9998):
        self.host = host
        self.port = port
        self.url = 'http://{}:{}'.format(host, port)

    def is_running(self):
        ip = socket.gethostbyname(self.host)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            r = sock.connect_ex((ip, self.port))
            if r == 0:
                sock.close()
                return True
            else:
                return False
        except socket.error as exc:
            mlgg.exception(exc)
            return False

    def version(self):
        url = self.url + '/version'
        return requests.get(url).text

    def detect(self, fn, hh):
        """
        Returns accurate content-type.

        :param fn: Filename.
        :param hh: Optional array with header fields for Tika server
        :returns: The content-type.
        :rtype: string
        """
        if hh is None:
            hh = {}
        hh.update({
            'content-disposition': 'attachment; filename={}'.format(fn)
        })
        url = self.url + '/detect/stream'
        r = self._send(url, fn, hh)
        return r.text

    def language(self, fn, hh):
        """
        Returns identified language as 2 chars.

        :param fn: Filename.
        :param hh: Optional array with header fields for Tika server
        :returns: The language.
        :rtype: string
        """
        if hh is None:
            hh = {}
        url = self.url + '/language/stream'
        r = self._send(url, fn, hh)
        return r.text

    def rmeta(self, fn, hh=None):
        """
        Returns recursive meta info about compound document.

        :param fn: Filename.
        :param hh: Optional array with header fields for Tika server
        :returns: List of dicts. Each dict has meta info about one of the
            compound documents. Key ``X-TIKA:content`` contains text document.
        :rtype: List of dicts
        """
        if hh is None:
            hh = {}
        url = self.url + '/rmeta'
        r = self._send(url, fn, hh)
        try:
            return r.json()
        except ValueError:
            return r.text

    def unpack(self, fn, all_=False, hh=None):
        """
        Unpacks compound document and returns ZIP archive.

        :param fn: Filename
        :param hh: Optional array with header fields for Tika server
        :param all_: Get all compound documents.
        :return: File-like bytestream.
        """
        if hh is None:
            hh = {}
        hh.update({
            'content-type': 'application/zip'
        })
        url = self.url + '/unpack'
        if all_:
            url += '/all'
        r = self._send(url, fn, hh)
        return io.BytesIO(r.content) if r.content else None

    def meta(self, fn, type_='json', hh=None):
        """
        Returns meta info.
        :param fn: Filename.
        :param hh: Optional array with header fields for Tika server
        :param type_: 'csv', 'json' or 'xmp'
        :return:
        """
        if not type_:
            type_ = 'json'
        if hh is None:
            hh = {}
        hh.update(self.__class__.TYPE_MAP[type_])
        url = self.url + '/meta'
        r = self._send(url, fn, hh)
        if type_ == 'json':
            try:
                return r.json()
            except ValueError:
                return r.text
        else:
            return r.text

    def tika(self, fn, type_='text', hh=None):
        """
        Returns text or HTML of content.

        :param fn: Filename.
        :param hh: Optional array with header fields for Tika server
        :param type_: 'text', 'html'
        :return: HTML or text
        """
        if hh is None:
            hh = {}
        hh.update(self.__class__.TYPE_MAP[type_])
        hh['Accept-Charset'] = 'unicode-1-1; q=1.0'
        url = self.url + '/tika'
        r = self._send(url, fn, hh)
        r.encoding = 'utf-8'
        return r.text

    @staticmethod
    def _send(url, fn, hh):
        """
        PUTs given file to URL.

        Also sets header content-disposition.

        :param url: Destination URL.
        :param fn: Filename
        :param hh: Optional array with header fields for Tika server
        :return: `request.Response`
        """
        hh['content-disposition'] = 'attachment; filename={}'.format(fn)
        with open(fn, 'rb') as fh:
            return requests.put(url, data=fh, headers=hh)
