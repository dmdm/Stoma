import sqlalchemy as sa
from sqlalchemy import engine_from_config, MetaData
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import (
    declarative_base)
from sqlalchemy.orm import (
    sessionmaker
)
from sqlalchemy.orm.collections import InstrumentedList

# or use the appropriate escape function from your db driver

from zope.sqlalchemy import ZopeTransactionExtension
from pym.models.types import LocalDateTime
import pym.lib
from .i18n import _
from .const import (MIME_TYPE_DEFAULT, IN_PROCESS_ITEM_STATES,
    ITEM_STATE_UNCHANGED)


# ===[ DB HELPERS ]=======

naming_convention = {
    "ix": '%(column_0_label)s_ix',
    "uq": "%(table_name)s_%(column_0_name)s_ux",
    ### "ck": "%(table_name)s_%(constraint_name)s_ck",
    "fk": "%(table_name)s_%(column_0_name)s_%(referred_table_name)s_fk",
    "pk": "%(table_name)s_pk"
}

DbSession = sessionmaker(
    extension=ZopeTransactionExtension()
)

"""
Factory for DB session.
"""
metadata = MetaData(
    naming_convention=naming_convention
)

DbBase = declarative_base(metadata=metadata)
"""
Our base class for declarative models.
"""
DbEngine = None
"""
Default DB engine.
"""


# ===[ IMPORTABLE SETUP FUNCS ]=======

def init(settings):
    """
    Initializes scoped SQLAlchemy by rc settings.

    Creates engine, binds a scoped session and declarative base.
    Call this function for global initialization of the WebApp.

    Initialises the module globals ``DbEngine``, ``DbSession`` and ``DbBase``.
    The session is joined into the Zope Transaction Manager.

    :param settings: Dict with settings
    :param prefix: Prefix for SQLAlchemy settings
    """
    global DbEngine
    # DbEngine = engine_from_config(
    #     settings, prefix,
    #     json_serializer=pym.lib.json_serializer,
    #     json_deserializer=pym.lib.json_deserializer
    # )
    DbEngine = engine_from_config(settings, prefix='')
    DbSession.configure(bind=DbEngine)
    DbBase.metadata.bind = DbEngine


def create_all():
    """Creates bound data model."""
    DbBase.metadata.create_all(DbEngine)


class Item(DbBase):
    __tablename__ = "item"
    __table_args__ = (
        {'schema': 'stoma'}
    )

    path = sa.Column(sa.Unicode(1024), nullable=False, primary_key=True)
    ela_id = sa.Column(sa.Unicode(1024), nullable=True)
    ela_version = sa.Column(sa.Unicode(1024), nullable=True)
    state = sa.Column(sa.Unicode(24), nullable=False,
        server_default=sa.text("'" + ITEM_STATE_UNCHANGED + "'"))

    size = sa.Column(
        sa.Integer(),
        sa.CheckConstraint('size>=0'),
        nullable=False,
        server_default=sa.text('0')
    )
    """Size of the content in bytes. Reflect this in FsNode.size."""
    item_ctime = sa.Column(sa.DateTime(), nullable=False)
    item_mtime = sa.Column(sa.DateTime(), nullable=False)

    mime_type = sa.Column(sa.Unicode(255), nullable=False,
        server_default=sa.text("'" + MIME_TYPE_DEFAULT + "'"))
    """Mime type. Reflect this in FsNode.mime_type."""
    encoding = sa.Column(sa.Unicode(255), nullable=True)
    """Encoding, if content is text."""
    language = sa.Column(sa.Unicode(255), nullable=True)
    """Detected language."""

    # noinspection PyUnusedLocal
    @sa.orm.validates('mime_type')
    def validate_mime_type(self, key, mime_type):
        mime_type = mime_type.lower()
        assert '/' in mime_type
        return mime_type

    os_stat = sa.Column(JSONB(none_as_null=True), nullable=True)
    """Result of os.stat"""

    xattr = sa.Column(JSONB(none_as_null=True), nullable=True)
    """Extended attributes"""

    meta_json = sa.Column(JSONB(none_as_null=True), nullable=True)
    """Extracted meta information as JSON"""
    meta_xmp = sa.Column(sa.UnicodeText(), nullable=True)
    """Extracted meta information as XMP"""
    data_text = sa.Column(sa.UnicodeText(), nullable=True)
    """Certain mime-types allow storing content as text. Also the text rendering
    of uploaded office documents is stored here."""
    data_json = sa.Column(JSONB(none_as_null=True), nullable=True)
    """Certain mime-types allow storing content as JSON"""
    data_html_head = sa.orm.deferred(sa.Column(sa.UnicodeText(), nullable=True))
    """Head of HTML rendering of office documents."""
    data_html_body = sa.orm.deferred(sa.Column(sa.UnicodeText(), nullable=True))
    """Body of HTML rendering of office documents."""

    def set_meta(self, meta):
        kk = 'meta_json meta_xmp data_text data_html_head data_html_body'.split(' ')
        mj = meta.get('meta_json', None)
        if mj:
            s = pym.lib.json_serializer(mj)
            s = s.replace("\0", '').replace("\x00", '').replace("\u0000", '').replace("\\u0000", '')
            meta['meta_json'] = pym.lib.json_deserializer(s)
        for k in kk:
            setattr(self, k, meta.get(k, None))

    ctime = sa.Column(LocalDateTime, server_default=sa.func.current_timestamp(),
        nullable=False,
            info={'colanderalchemy': {'title': _("Creation Time")}})
    """Timestamp, creation time."""
    mtime = sa.Column(LocalDateTime, nullable=True,
            info={'colanderalchemy': {'title': _("Mod Time")}})
    """Timestamp, last edit time."""


# Do not walk over items currently processed by other tasks
def exclude_filter():
    return [Item.state != st for st in IN_PROCESS_ITEM_STATES]
