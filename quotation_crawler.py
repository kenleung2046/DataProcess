import tushare as ts
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
import traceback
import time

_token_ = '52b3d356995423a9f694a1bb792f789386d2e88f02d5b47bbf5a7e22'
pro = ts.pro_api(_token_)
api = ts.pro_api(_token_)

_database_ip_ = '127.0.0.1'
_database_port_ = 27017
_authentication_ = 'A-Shares'
_user_ = 'manager'
_pwd_ = 'Kl!2#4%6'
_database_name_ = 'A-Shares'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def crawl_quotation_daily(begin_date, end_date, adj=None):
    basic_cursor = db.Stock_Basic.find(
        {'list_status': 'L',
         'list_date': {'$lte': end_date}},
        sort=[('ts_code', ASCENDING)],
        projection={'_id': False, 'ts_code': True}
    )
    codes = [x['ts_code'] for x in basic_cursor]

    inserted_amount = 0
    updated_amount = 0

    _collection_name_ = 'Quotation_Daily' if adj is None else 'Quotation_Daily_' + adj

    # 网上调取数据次数控制
    request_api_times = 0

    for code in codes:
        try:
            df_quotation = ts.pro_bar(
                pro_api=api, ts_code=code, start_date=begin_date, end_date=end_date, asset='E', adj=adj, freq='D'
            )

            update_requests = []
            for index in df_quotation.index:
                document = dict(df_quotation.loc[index])

                update_requests.append(
                    UpdateOne(
                        {'ts_code': code,
                         'trade_date': document['trade_date']},
                        {'$set': document},
                        upsert=True
                    )
                )

            if len(update_requests) > 0:
                update_result = db[_collection_name_].bulk_write(
                    update_requests,
                    ordered=False
                )

                print(
                    'Update %s from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d' %
                    (code, begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
                     update_result.upserted_count, update_result.modified_count),
                    flush=True
                )

                inserted_amount = inserted_amount + update_result.upserted_count
                updated_amount = updated_amount + update_result.modified_count

            # 网上调取数据次数控制
            request_api_times += 1
            if request_api_times % 200 == 0:
                print('crawl data requests api times control')
                time.sleep(70)

        except:
            print(
                'Error occurs when update stock quotation from %s to %s, %s-%s, %s at position input: %s' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(), code),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_update_"
                + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Error occurs when update stock quotation from %s to %s, %s-%s , %s at position input: %s \n \n' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(), code) + content
            )
            traceback.print_exc(file=_log)
            _log.flush()
            _log.close()

    print('total inserted amount is %s, total modified amount is %s' % (inserted_amount, updated_amount))
    _log = open(
        "/root/DataProcess/log/log_update_"
        + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
    )
    content = _log.read()
    _log.seek(0, 0)
    _log.write(
        'Update stock quotation from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
        (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


if __name__ == '__main__':
    crawl_quotation_daily('20190301', '20190301')
    time.sleep(70)
    crawl_quotation_daily('20190301', '20190301', adj='hfq')
    time.sleep(70)
    crawl_quotation_daily('20190301', '20190301', adj='qfq')
