from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime


_database_ip_ = '127.0.0.1'
_database_port_ = 27017
_authentication_ = 'A-Shares'
_user_ = 'manager'
_pwd_ = 'Kl!2#4%6'
_database_name_ = 'A-Shares'
_collection_name_ = 'Adj_Factor'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def adj_factor_compute(begin_date, end_date):
    calendar_cursor = db.Calendar.find(
        {
            'cal_date': {'$gte': begin_date, '$lte': end_date},
            'is_open': True
        },
        sort=[('cal_date', ASCENDING)],
        projection={'cal_date': True, '_id': False},
    )
    dates = [x['cal_date'] for x in calendar_cursor]

    inserted_amount = 0
    updated_amount = 0

    for date in dates:
        quotation_cursor = db.Quotation_Daily.find(
            {'trade_date': date},
            sort=[('ts_code', ASCENDING)],
            projection={'ts_code': True, 'close': True, '_id': False},
            batch_size=4000
        )

        update_requests = []
        for quotation in quotation_cursor:
            code = quotation['ts_code']
            close = quotation['close']
            hfq_close = db.Quotation_Daily_hfq.find_one({'trade_date': date, 'ts_code': code})['close']
            adj_factor = hfq_close/close
            document = {
                'trade_date': date,
                'ts_code': code,
                'adj_factor': adj_factor
            }
            update_requests.append(
                UpdateOne(
                    {'trade_date': date,
                     'ts_code': code},
                    {'$set': document},
                    upsert=True
                ))

        if len(update_requests) > 0:
            update_result = db[_collection_name_].bulk_write(
                update_requests,
                ordered=False
            )

            print(
                'Compute Adjusted Factor at date: %s, %s-%s, %s, inserted: %4d, modified: %4d' %
                (date, _database_name_, _collection_name_, datetime.now(),
                 update_result.upserted_count, update_result.modified_count),
                flush=True
            )

            inserted_amount = inserted_amount + update_result.upserted_count
            updated_amount = updated_amount + update_result.modified_count

    print('total inserted amount is %s, total modified amount is %s' % (inserted_amount, updated_amount))
    _log = open(
        "/root/DataProcess/log/log_compute_"
        + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
    )
    content = _log.read()
    _log.seek(0, 0)
    _log.write(
        'Compute Adjusted Factor from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
        (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


if __name__ == '__main__':
    adj_factor_compute('20050101', '20190314')
