import tushare as ts
from pymongo import MongoClient, UpdateOne, ASCENDING
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
_collection_name_ = 'suspension'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def crawl_suspension(begin_date, end_date):
    calendar_cursor = db.calendar.find(
        {
            'cal_date': {'$gte': begin_date, '$lte': end_date},
            'is_open': True
        },
        sort=[('cal_date', ASCENDING)],
        projection={'_id': False, 'cal_date': True}
    )
    dates = [x['cal_date'] for x in calendar_cursor]

    inserted_amount = 0
    updated_amount = 0

    for date in dates:
        try:
            df_suspension = pro.suspend(
                ts_code='', suspend_date=date, resume_date='',
                fields='ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type'
            )

            update_requests = []
            for index in df_suspension.index:
                document = dict(df_suspension.loc[index])

                update_requests.append(
                    UpdateOne(
                        {'suspend_date': date,
                         'ts_code': document['ts_code']},
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
                    'Update at date: %s all stock suspension, %s-%s at %s, inserted: %4d, modified: %4d' %
                    (date, _database_name_, _collection_name_, datetime.now(),
                     update_result.upserted_count, update_result.modified_count),
                    flush=True
                )

                inserted_amount = inserted_amount + update_result.upserted_count
                updated_amount = updated_amount + update_result.modified_count

        except:
            print(
                'Error occurs when update stock suspension from %s to %s, %s-%s, %s at position input: %s' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(), date),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_update_"
                + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Error occurs when update stock suspension from %s to %s, %s-%s, %s at position input: %s \n \n' %
                (begin_date, end_date, _database_name_, _collection_name_, datetime.now(), date) + content
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
        'Update stock suspension from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
        (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


if __name__ == '__main__':
    crawl_suspension('20190307', '20190307')
