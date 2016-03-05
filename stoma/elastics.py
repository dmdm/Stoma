import requests
import socket
import logging
from pym.lib import json_serializer, json_deserializer


mlgg = logging.getLogger(__name__)


class ElasticSearchRestClient:

    def __init__(self, lgg, host='localhost', port=9200):
        self.lgg = lgg
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

    def hello(self):
        url = self.url
        r = requests.get(url)
        r.raise_for_status()
        return json_deserializer(r.text)

    def version(self):
        d = self.hello()
        return 'Cluster {} {} ({}), Lucene {}. {}'.format(
            d['cluster_name'],
            d['version']['number'],
            d['name'],
            d['version']['lucene_version'],
            d['tagline']
        )

    def count(self):
        url = self.url + '/_count'
        q = {
            "query": {
                "match_all": {}
            }
        }
        r = requests.get(url, data=json_serializer(q))
        r.raise_for_status()
        return json_deserializer(r.text)

    def save(self, index, doc_type, data, id_=None, create=None):
        if create and not id_:
            raise ValueError('To force document creation, id must be given')
        if id_:
            url = '{base}/{index}/{doc_type}/{id}'.format(
                base=self.url, index=index, doc_type=doc_type, id=id_
            )
            if create:
                url += '/_create'
            s = json_serializer(data).encode('utf-8')
            r = requests.put(url, data=s)
        else:
            url = '{base}/{index}/{doc_type}/'.format(
                base=self.url, index=index, doc_type=doc_type
            )
            s = json_serializer(data).encode('utf-8')
            r = requests.post(url, data=s)
        r.raise_for_status()
        return json_deserializer(r.text)

    def load(self, index, doc_type, id_, source=None):
        """
        Loads specified document.

        :param index: Index
        :param doc_type: Document type
        :param id_: ID
        :param source: Optionally restrict to some or all source fields, i.e. no
            meta data. If ``source`` is True, returns all source fields, else
            ``source`` must be an iterable of field names.
        :return: JSON
        """
        params = {}
        if not source:
            url = '{base}/{index}/{doc_type}/{id}'.format(
                base=self.url, index=index, doc_type=doc_type, id=id_
            )
        elif source is True:
            url = '{base}/{index}/{doc_type}/{id}/_source'.format(
                base=self.url, index=index, doc_type=doc_type, id=id_
            )
        else:
            url = '{base}/{index}/{doc_type}/{id}'.format(
                base=self.url, index=index, doc_type=doc_type, id=id_
            )
            params = dict(_source=source.join(','))
        r = requests.get(url, params=params)
        r.raise_for_status()
        return json_deserializer(r.text)

    def exists(self, index, doc_type, id_):
        url = '{base}/{index}/{doc_type}/{id}'.format(
            base=self.url, index=index, doc_type=doc_type, id=id_
        )
        r = requests.head(url)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False
        else:
            r.raise_for_status()

    def delete(self, index, doc_type, id_):
        url = '{base}/{index}/{doc_type}/{id}'.format(
            base=self.url, index=index, doc_type=doc_type, id=id_
        )
        r = requests.delete(url)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False
        else:
            r.raise_for_status()

    def search(self, index, doc_type, q):
        url = '{base}/{index}/{doc_type}/_search'.format(
            base=self.url, index=index, doc_type=doc_type
        )
        if isinstance(q, str):
            r = requests.get(url, params=dict(q=q))
        else:
            r = requests.get(url, data=json_serializer(q))
        r.raise_for_status()
        return json_deserializer(r.text)

    def create_index(self, index, rc=None):
        url = '{base}/{index}/'.format(
            base=self.url, index=index
        )
        s = json_serializer(rc) if rc else None
        r = requests.put(url, data=s)
        r.raise_for_status()
        return json_deserializer(r.text)

    def delete_index(self, index):
        url = '{base}/{index}/'.format(
            base=self.url, index=index
        )
        r = requests.delete(url)
        r.raise_for_status()
        return json_deserializer(r.text)
