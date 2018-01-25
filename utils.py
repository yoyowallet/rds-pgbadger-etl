import hashlib
from typing import Any, Sequence

import re


def hash_list(l: Sequence[Any]) -> str:
    m = hashlib.md5()
    for item in sorted(l):
        m.update(str(item).encode('utf-8'))
    return m.hexdigest()


date_format = re.compile(r'(\d{4}-\d{2}-\d{2})')


def extract_date_from_log_file_name(file_name: str) -> str:
    try:
        return date_format.search(file_name).group(0)
    except AttributeError:
        return 'unknown'
