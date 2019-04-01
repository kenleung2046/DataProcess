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
_collection_name_ = 'Calendar'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def crawl_calender(begin_date, end_date):
    try:
        df_calendar = pro.trade_cal(
            exchange='', start_date=begin_date, end_date=end_date
        )

        update_requests = []
        for index in df_calendar.index:
            document = dict(df_calendar.loc[index])

            is_open = False if document['is_open'] == 0 else True
            document.update({
                'is_open': is_open,
            })

            update_requests.append(
                UpdateOne(
                    {'cal_date': document['cal_date']},
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
                'Update calendar from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d' %
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
                'Update calendar from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
                 update_result.upserted_count, update_result.modified_count) + content
            )
            _log.flush()
            _log.close()

    except:
        print(
            'Error occurs when update calendar from %s to %s, %s-%s, %s' %
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
            'Error occurs when update calendar from %s to %s, %s-%s, %s \n \n' %
            (begin_date, end_date, _database_name_, _collection_name_, datetime.now()) + content
        )
        traceback.print_exc(file=_log)
        _log.flush()
        _log.close()


if __name__ == '__main__':
    crawl_calender('20190307', '20190307')
