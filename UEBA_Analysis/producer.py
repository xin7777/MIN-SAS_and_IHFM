# -----------------------------------------------------------------------------
# Copyright (C) 2019-2020 The python-ndn authors
#
# This file is part of python-ndn.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------
import log_parser
import ueba_analyze
from typing import Optional
from ndn.app import NDNApp
from ndn.encoding import Name, InterestParam, BinaryStr, FormalName, MetaInfo

import logging
from multiprocessing import Queue
from threading import Thread
import time

logging.basicConfig(format='[{asctime}]{levelname}:{message}',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    style='{')

app = NDNApp()


@app.route('/MIN-VPN/testflow/SAS')
def on_interest(name: FormalName, param: InterestParam, _app_param: Optional[BinaryStr]):
    print(f'>> I: {Name.to_str(name)}, {param}, {_app_param[:].tobytes().decode()}')
    content = "Thanks".encode()
    app.put_data(name, content=content, freshness_period=10000)
    # count += 1
    # print(f'{count}th packet received')
    # print(f'<< D: {Name.to_str(name)}')
    # print(MetaInfo(freshness_period=10000))
    # print(f'Content: (size: {len(content)})')
    # print('')
    # queue.put(_app_param[:].tobytes().decode())
    # print(f'queue size: {queue.qsize()}')


def consumer(q):
    print('start waiting vpn server logs...')
    # while 1:
    #     time.sleep(10)
    #     print('start getting vpn server logs...')
    #     print(f'queue size: {q.qsize()}')
    #     if q.qsize() == 0:
    #         print('no log received, keep waiting...')
    #         continue
    #
    #     logs = []
    #     while q.qsize() > 0:
    #         logs.append(q.get())
    #
    #     # TODO 分析日志
    #     parser = log_parser.LogParser(logs)
    #     df = parser.dict2dataframe()
    #     print(df)
    #     ueba_analyze.run(df)


if __name__ == '__main__':
    # pro = Thread(target=app.run_forever, args=())
    #con = Thread(target=consumer, args=(queue,))
    # pro.start()
    #con.start()
    app.run_forever()
