import tushare as ts
from pymongo import UpdateOne, MongoClient
from datetime import datetime
import traceback

_token_ = '52b3d356995423a9f694a1bb792f789386d2e88f02d5b47bbf5a7e22'
pro = ts.pro_api(_token_)

_database_ip_ = '127.0.0.1'
_database_port_ = 27017
_authentication_ = 'A-Shares'
_user_ = 'manager'
_pwd_ = 'Kl!2#4%6'
_database_name_ = 'A-Shares'
_collection_name_ = 'stock_basic'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def crawl_basic(list_status=None, update_date='20190401'):

    stock_status = 'listed' if list_status is None else 'de-listed'

    try:
        df_basic = pro.stock_basic(
            exchange='', list_status=list_status,
            fields='ts_code,symbol,name,area,industry,fullname,'
                   'enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        )

        update_requests = []
        for index in df_basic.index:
            document = dict(df_basic.loc[index])

            document.update({
                'update_date': update_date,
            })

            update_requests.append(
                UpdateOne(
                    {'ts_code': document['ts_code']},
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
                'Update %s stock basics, %s-%s, %s, inserted: %4d, modified: %4d' %
                (stock_status, _database_name_, _collection_name_, datetime.now(),
                 update_result.upserted_count, update_result.modified_count),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_update_"
                + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Update %s stock basics, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
                (stock_status, _database_name_, _collection_name_, datetime.now(),
                 update_result.upserted_count, update_result.modified_count) + content
            )
            _log.flush()
            _log.close()

    except:
        print(
            'Error occurs when update %s stock basics, %s-%s, %s' %
            (stock_status, _database_name_, _collection_name_, datetime.now()),
            flush=True
        )

        _log = open(
            "/root/DataProcess/log/log_update_"
            + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
        )
        content = _log.read()
        _log.seek(0, 0)
        _log.write(
            'Error occurs when update %s stock basics, %s-%s, %s \n \n' %
            (stock_status, _database_name_, _collection_name_, datetime.now()) + content
        )
        traceback.print_exc(file=_log)
        _log.flush()
        _log.close()


if __name__ == '__main__':
    crawl_basic(update_date='20190401')
    crawl_basic(list_status='D', update_date='20190401')
    crawl_basic(list_status='P', update_date='20190401')
