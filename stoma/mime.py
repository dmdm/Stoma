import magic
import mimetypes


def guess_mime_type(fn,
        magic_inst=magic.Magic(mime=True, mime_encoding=True, keep_going=True)):
    """
    Guesses mime-type from filename.

    Uses Python's lib ``mimetypes`` first, if no type could be determined, falls
    back to ``python-magic``.

    Returned encoding might be None.

    :param fn: Filename.
    :param magic_inst: Instance of :class:`magic.Magic`. Should be created with
        mime=True, mime_encoding=True, keep_going=True.
    :return: Tuple(mime_type, encoding).
    """
    # Try Python's native lib first
    mt, enc = mimetypes.guess_type(fn)
    # It may not find all types, e.g. it returns None for 'text/plain', so
    # fallback on python-magic.
    if not mt:
        if magic_inst:
            mt = magic_inst.from_file(fn).decode('ASCII')
        else:
            mt = magic.from_file(fn).decode('ASCII')
    if not enc:
        enc = None
    # In case magic returned several types on separate lines
    mt = mt.split(r'\012')[0]
    return mt, enc
