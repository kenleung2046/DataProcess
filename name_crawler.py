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
_collection_name_ = 'name_his'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def name_his_crawler(begin_date, end_date):
    try:
        df_name = pro.namechange(
            start_date=begin_date, end_date=end_date, fields='ts_code,name,start_date,end_date,ann_date,change_reason'
        )

        update_requests = []
        for index in df_name.index:
            document = dict(df_name.loc[index])
            update_requests.append(
                UpdateOne(
                    {'ts_code': document['ts_code'],
                     'start_date': document['start_date']},
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
                'Update stock historical name from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
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
                'Update stock historical name from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
                 update_result.upserted_count, update_result.modified_count) + content
            )
            _log.flush()
            _log.close()

    except:
        print(
            'Error occurs when update stock historical name from %s to %s, %s-%s, %s' %
            (begin_date, end_date, _database_name_, _collection_name_, datetime.now()),
            flush=True
        )

        _log = open(
            "/root/DataProcess/log/log_update_"
            + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
        )
        content = _log.read()
        _log.seek(0, 0)
        _log.write(
            'Error occurs when update stock historical name from %s to %s, %s-%s, %s \n \n' %
            (begin_date, end_date, _database_name_, _collection_name_, datetime.now()) + content
        )
        traceback.print_exc(file=_log)
        _log.flush()
        _log.close()


if __name__ == '__main__':
    name_his_crawler('20050101', '20190308')
