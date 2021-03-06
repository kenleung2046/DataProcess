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
_collection_name_ = 'disclosure_date'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def crawl_disclosure_date(end_date):
    basic_cursor = db.stock_basic.find(
        {'list_status': 'L',
         'list_date': {'$lte': end_date}},
        sort=[('ts_code', ASCENDING)],
        projection={'_id': False, 'ts_code': True}
    )
    codes = [x['ts_code'] for x in basic_cursor]

    inserted_amount = 0
    updated_amount = 0

    # 网上调取数据次数控制
    request_api_times = 0

    for code in codes:
        try:
            df_disclosure = pro.disclosure_date(ts_code=code, end_date=end_date)

            update_requests = []
            for index in df_disclosure.index:
                document = dict(df_disclosure.loc[index])

                update_requests.append(
                    UpdateOne(
                        {'ts_code': code,
                         'end_date': document['end_date']},
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
                        'Update %s at %s, %s-%s, %s, inserted: %4d, modified: %4d' %
                        (code, end_date, _database_name_, _collection_name_, datetime.now(),
                         update_result.upserted_count, update_result.modified_count),
                        flush=True
                    )

                    inserted_amount = inserted_amount + update_result.upserted_count
                    updated_amount = updated_amount + update_result.modified_count

                # 网上调取数据次数控制
                request_api_times += 1
                if request_api_times % 100 == 0:
                    print('crawl data requests api times control')
                    time.sleep(70)

        except:
            print(
                'Error occurs when update stock disclosure date at %s, %s-%s, %s at position input: %s' %
                (end_date, _database_name_, _collection_name_, datetime.now(), code),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_update_"
                + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Error occurs when update stock disclosure date at %s, %s-%s , %s at position input: %s \n \n' %
                (end_date, _database_name_, _collection_name_, datetime.now(), code) + content
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
        'Update stock disclosure date at %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
        (end_date, _database_name_, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


if __name__ == '__main__':
    crawl_disclosure_date('20181231')
