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
_collection_name_ = 'Quantification'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def atr_compute(begin_date, end_date):
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
                projection={'trade_date': True, 'close': True, 'high': True, 'low': True, 'pre_close': True,
                            '_id': False}
            )
            quotation = [x for x in quotation_cursor]

            adj_factor_cursor = db.Adj_Factor.find(
                {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}},
                sort=[('trade_date', ASCENDING)],
                projection={'trade_date': True, 'adj_factor': True, '_id': False}
            )
            adj_factor_list = [factor for factor in adj_factor_cursor]

            update_requests = []
            if len(quotation) and len(adj_factor_list) is not 0:

                df_adj_factor = DataFrame([adj for adj in adj_factor_list])
                df_adj_factor.set_index(['trade_date'], 1, inplace=True)

                df_quotation = DataFrame([y for y in quotation])

                df_quotation['H-L'] = round(df_quotation['high'] - df_quotation['low'], 5)
                df_quotation['H-PDC'] = round(df_quotation['high'] - df_quotation['pre_close'], 5)
                df_quotation['PDC-L'] = round(df_quotation['pre_close'] - df_quotation['low'], 5)

                df_quotation['TR'] = df_quotation.iloc[:, 5:7].max(axis=1)

                last_atr = round(df_quotation['TR'].loc[0:19].mean(), 5)

                for index in range(20, len(df_quotation)):

                    tr = df_quotation.loc[index]['TR']

                    atr = (19 * last_atr + tr) / 20

                    date = df_quotation['trade_date'].loc[index]

                    atr_adj_factor = round(atr / df_adj_factor['adj_factor'].loc[date], 2)

                    tr_adj_factor = round(tr / df_adj_factor['adj_factor'].loc[date], 2)

                    update_requests.append(
                        UpdateOne(
                            {'ts_code': code, 'trade_date': date},
                            {'$set': {'ts_code': code, 'trade_date': date,
                                      'adj_TR': tr_adj_factor, 'adj_ATR': atr_adj_factor}},
                            upsert=True)
                    )

                    last_atr = atr

            else:
                pass

            if len(update_requests) > 0:
                update_result = db[_collection_name_].bulk_write(
                    update_requests,
                    ordered=False
                )

                print(
                    'Compute %s daily adj_TR and adj_ATR from %s to %s, %s, %s, inserted: %4d, modified: %4d' %
                    (code, begin_date, end_date, _collection_name_, datetime.now(),
                     update_result.upserted_count, update_result.modified_count),
                    flush=True
                )

                inserted_amount = inserted_amount + update_result.upserted_count
                updated_amount = updated_amount + update_result.modified_count

        except:
            print(
                'Error occurs when compute daily adj_TR and adj_ATR from %s to %s, %s, %s, at position: %s' %
                (begin_date, end_date, _collection_name_, datetime.now(), code),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_compute_"
                + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Error occurs when compute daily adj_TR and adj_ATR from %s to %s, %s, %s, at position: %s \n \n' %
                (begin_date, end_date, _collection_name_, datetime.now(), code) + content
            )
            traceback.print_exc(file=_log)
            _log.flush()
            _log.close()

    print('total inserted amount is %s, total modified amount is %s' % (inserted_amount, updated_amount))
    _log = open(
        "/root/DataProcess/log/log_compute_"
        + _collection_name_ + ".txt", 'r+'
    )
    content = _log.read()
    _log.seek(0, 0)
    _log.write(
        'Compute daily adj_TR and adj_ATR from %s to %s, %s, %s, inserted: %4d, modified: %4d \n \n' %
        (begin_date, end_date, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


def atr_compute_daily(current_date):
    code_cursor = db.Stock_Basic.find(
        {'list_status': 'L',
         'list_date': {'$lte': current_date}},
        projection={'ts_code': True, '_id': False}
    )
    codes = [code['ts_code'] for code in code_cursor]

    calendar_cursor = db.Calendar.find(
        {'cal_date': {'$gte': '20190101', '$lte': current_date}, 'is_open': True},
        sort=[('cal_date', ASCENDING)],
        projection={'cal_date': True, '_id': False},
    )
    calendar = [x for x in calendar_cursor]

    df_calendar = DataFrame([y for y in calendar])

    update_requests = []
    for code in codes:
        try:
            quotation = db.Quotation_Daily_hfq.find_one(
                {'ts_code': code, 'trade_date': current_date, 'is_trading': True}
            )
            adj_factor = db.Adj_Factor.find_one(
                {'ts_code': code, 'trade_date': current_date}
            )
            last_date = df_calendar['cal_date'].iloc[-2]
            last_atr = db.Quantification.find_one({'ts_code': code, 'trade_date': last_date})

            if quotation and adj_factor and last_atr is not None:

                high_low = round(quotation['high'] - quotation['low'], 5)
                high_pdc = round(quotation['high'] - quotation['pre_close'], 5)
                pdc_low = round(quotation['pre_close'] - quotation['low'], 5)
                tr = max(high_low, high_pdc, pdc_low)
                tr_adj = round(tr / adj_factor['adj_factor'], 2)
                atr_adj = round((19 * last_atr['adj_ATR'] + tr_adj) / 20, 2)

                update_requests.append(
                    UpdateOne(
                        {'ts_code': code, 'trade_date': current_date},
                        {'$set': {'ts_code': code, 'trade_date': current_date,
                                  'adj_TR': tr_adj, 'adj_ATR': atr_adj}},
                        upsert=True)
                )

            else:
                pass

        except:
            print(
                'Error occurs when compute daily adj_TR and adj_ATR from %s to %s, %s, %s, at position: %s' %
                (current_date, current_date, _collection_name_, datetime.now(), code),
                flush=True
            )

            _log = open(
                "/root/DataProcess/log/log_compute_"
                + _collection_name_ + ".txt", 'r+'
            )
            content = _log.read()
            _log.seek(0, 0)
            _log.write(
                'Error occurs when compute daily adj_TR and adj_ATR from %s to %s, %s, %s, at position: %s \n \n' %
                (current_date, current_date, _collection_name_, datetime.now(), code) + content
            )
            traceback.print_exc(file=_log)
            _log.flush()
            _log.close()

    if len(update_requests) > 0:
        update_result = db[_collection_name_].bulk_write(
            update_requests,
            ordered=False
        )

        print(
            'Compute daily adj_TR and adj_ATR from %s to %s, %s, %s, inserted: %4d, modified: %4d' %
            (current_date, current_date, _collection_name_, datetime.now(),
             update_result.upserted_count, update_result.modified_count),
            flush=True
        )
        print('total inserted amount is %s, total modified amount is %s' %
              (update_result.upserted_count, update_result.modified_count))
        _log = open(
            "/root/DataProcess/log/log_compute_"
            + _collection_name_ + ".txt", 'r+'
        )
        content = _log.read()
        _log.seek(0, 0)
        _log.write(
            'Compute daily adj_TR and adj_ATR from %s to %s, %s, %s, inserted: %4d, modified: %4d \n \n' %
            (current_date, current_date, _collection_name_, datetime.now(),
             update_result.upserted_count, update_result.modified_count) + content
        )
        _log.flush()
        _log.close()


if __name__ == '__main__':
    atr_compute('20050101', '20190411')
    atr_compute_daily('20190412')
