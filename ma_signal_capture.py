from pymongo import MongoClient, ASCENDING, UpdateOne
from pandas import DataFrame
from datetime import datetime
import traceback


_database_ip_ = '127.0.0.1'
_database_port_ = 27017
_authentication_ = 'A-Shares'
_user_ = 'manager'
_pwd_ = 'Kl!2#4%6'
_database_name_ = 'A-Shares'
_collection_name_ = 'Signal'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def get_ma_signal(begin_date, end_date, ma_days):
    code_cursor = db.Stock_Basic.find(
        {'list_status': 'L',
         'list_date': {'$lte': end_date}},
        projection={'ts_code': True, '_id': False}
    )
    codes = [code['ts_code'] for code in code_cursor]

    inserted_amount = 0
    updated_amount = 0

    for code in codes:
        try:
            quotation_cursor = db.Quotation_Daily_hfq.find(
                {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}, 'is_trading': True},
                sort=[('trade_date', ASCENDING)],
                projection={'trade_date': True, 'close': True, '_id': False}
            )

            df_quotation = DataFrame([quotation for quotation in quotation_cursor])

            df_quotation.set_index(['trade_date'], 1, inplace=True)

            df_quotation['ma'] = round(df_quotation['close'].rolling(ma_days).mean(), 5)

            df_quotation['differ'] = round(df_quotation['close'] - df_quotation['ma'], 5)

            df_quotation['differ_prev'] = df_quotation['differ'].shift(1)

            df_quotation['up_break'] = (df_quotation['differ_prev'] <= 0) & (df_quotation['differ'] > 0)

            df_quotation['down_break'] = (df_quotation['differ_prev'] >= 0) & (df_quotation['differ'] < 0)

            df_quotation.drop(['close', 'ma', 'differ', 'differ_prev'], 1, inplace=True)

            df_quotation = df_quotation[df_quotation['up_break'] | df_quotation['down_break']]

            signal_name = 'signal_ma'
            if ma_days == 5:
                signal_name = 'signal_ma_5'
            elif ma_days == 10:
                signal_name = 'signal_ma_10'
            elif ma_days == 20:
                signal_name = 'signal_ma_20'
            elif ma_days == 30:
                signal_name = 'signal_ma_30'
            elif ma_days == 60:
                signal_name = 'signal_ma_60'
            elif ma_days == 120:
                signal_name = 'signal_ma_120'
            elif ma_days == 240:
                signal_name = 'signal_ma_240'

            update_requests = []
            for date in df_quotation.index:
                signal = 'up_break' if df_quotation.loc[date]['up_break'] else 'down_break'
                update_requests.append(
                    UpdateOne(
                        {'ts_code': code, 'trade_date': date},
                        {'$set': {'ts_code': code, 'trade_date': date, signal_name: signal}},
                        upsert=True)
                )

            if len(update_requests) > 0:
                update_result = db[_collection_name_].bulk_write(
                    update_requests,
                    ordered=False
                )

                print(
                    'Compute %s Ma_%s signal from %s to %s, %s, %s, inserted: %4d, modified: %4d' %
                    (code, ma_days, begin_date, end_date, _collection_name_, datetime.now(),
                     update_result.upserted_count, update_result.modified_count),
                    flush=True
                )

                inserted_amount = inserted_amount + update_result.upserted_count
                updated_amount = updated_amount + update_result.modified_count

        except:
            print(
                'Error occurs when compute Ma_%s signal from %s to %s, %s, %s, at position: %s' %
                (ma_days, begin_date, end_date, _collection_name_, datetime.now(), code),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_save_compute_"
                + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Error occurs when compute Ma_%s signal from %s to %s, %s, %s, at position: %s \n \n' %
                (ma_days, begin_date, end_date, _collection_name_, datetime.now(), code) + content
            )
            traceback.print_exc(file=_log)
            _log.flush()
            _log.close()

    print('total inserted amount is %s, total modified amount is %s' % (inserted_amount, updated_amount))
    _log = open(
        "/root/DataProcess/log/log_save_compute_"
        + _collection_name_ + ".txt", 'r+'
    )
    content = _log.read()
    _log.seek(0, 0)
    _log.write(
        'Compute Ma_%s signal from %s to %s, %s, %s, inserted: %4d, modified: %4d \n \n' %
        (ma_days, begin_date, end_date, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


if __name__ == '__main__':
    get_ma_signal('20050101', '20190307', 5)
    # get_ma_signal('20050101', '20190307', 10)
    # get_ma_signal('20050101', '20190307', 20)
    # get_ma_signal('20050101', '20190307', 30)
    # get_ma_signal('20050101', '20190307', 60)
    # get_ma_signal('20050101', '20190307', 120)
    # get_ma_signal('20050101', '20190307', 240)
