import unittest

import base64
a = 'd3pxMTIz'
missing_padding = 4 - len(a) % 4
if missing_padding:
    a += '=' * missing_padding
res = base64.b64decode(a)
print(str(res, 'utf-8'))