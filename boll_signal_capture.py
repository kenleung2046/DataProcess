from pymongo import MongoClient, ASCENDING, UpdateOne
from pandas import DataFrame
from datetime import datetime
import traceback


class BollSignal:
    def __init__(self):
        _database_ip_ = '127.0.0.1'
        _database_port_ = 27017
        _authentication_ = 'A-Shares'
        _user_ = 'manager'
        _pwd_ = 'Kl!2#4%6'
        _database_name_ = 'A-Shares'
        _client = MongoClient(_database_ip_, _database_port_)
        db_auth = _client[_authentication_]
        db_auth.authenticate(_user_, _pwd_)
        self.db = _client[_database_name_]
        
    def compute(self, begin_date, end_date, boll_days, boll_k):
        code_cursor = self.db.Stock_Basic.find(
            {'list_status': 'L',
             'list_date': {'$lte': end_date}},
            projection={'ts_code': True, '_id': False}
        )
        codes = [code['ts_code'] for code in code_cursor]

        inserted_amount = 0
        updated_amount = 0

        _collection_name_ = 'Signal'

        for code in codes:
            try:
                quotation_cursor = self.db.Quotation_Daily_hfq.find(
                    {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}, 'is_trading': True},
                    sort=[('trade_date', ASCENDING)],
                    projection={'trade_date': True, 'close': True, '_id': False}
                )
                quotation_list = [x for x in quotation_cursor]

                if len(quotation_list) is not 0:
                
                    df_quotation = DataFrame([quotation for quotation in quotation_list])

                    df_quotation.set_index(['trade_date'], 1, inplace=True)

                    df_quotation['mid'] = round(df_quotation['close'].rolling(boll_days).mean(), 5)

                    df_quotation['std'] = round(df_quotation['close'].rolling(boll_days).std(), 5)

                    df_quotation['up'] = round(df_quotation['mid'] + boll_k * df_quotation['std'], 5)
                    df_quotation['down'] = round(df_quotation['mid'] - boll_k * df_quotation['std'], 5)

                    df_quotation['up_delta'] = round(df_quotation['close'] - df_quotation['up'], 5)
                    df_quotation['up_delta_prev'] = df_quotation['up_delta'].shift(1)
                    df_quotation['up_break'] = (df_quotation['up_delta_prev'] <= 0) & (df_quotation['up_delta'] > 0)

                    df_quotation['down_delta'] = round(df_quotation['close'] - df_quotation['down'], 5)
                    df_quotation['down_delta_prev'] = df_quotation['down_delta'].shift(1)
                    df_quotation['down_break'] = (df_quotation['down_delta_prev'] >= 0) & (df_quotation['down_delta'] < 0)

                    df_quotation.drop(['close', 'mid', 'std', 'up', 'down', 'up_delta', 'down_delta', 'up_delta_prev',
                                       'down_delta_prev'], 1, inplace=True)

                    df_quotation = df_quotation[df_quotation['up_break'] | df_quotation['down_break']]

                    signal_name = 'signal_boll'
                    if (boll_days == 20) & (boll_k == 2):
                        signal_name = 'signal_boll_n20_k2'

                    update_requests = []
                    for date in df_quotation.index:
                        signal = 'up_break' if df_quotation.loc[date]['up_break'] else 'down_break'
                        update_requests.append(
                            UpdateOne(
                                {'ts_code': code, 'trade_date': date},
                                {'$set': {'ts_code': code, 'trade_date': date, signal_name: signal}},
                                upsert=True)
                        )

                else:
                    update_requests = []
                    
                if len(update_requests) > 0:
                    update_result = self.db[_collection_name_].bulk_write(
                        update_requests,
                        ordered=False
                    )

                    print(
                        'Compute %s Boll_N%s_K%s signal from %s to %s, %s, %s, inserted: %4d, modified: %4d' %
                        (code, boll_days, boll_k, begin_date, end_date, _collection_name_, datetime.now(),
                         update_result.upserted_count, update_result.modified_count),
                        flush=True
                    )

                    inserted_amount = inserted_amount + update_result.upserted_count
                    updated_amount = updated_amount + update_result.modified_count

            except:
                print(
                    'Error occurs when compute Boll_N%s_K%s signal from %s to %s, %s, %s, at position: %s' %
                    (boll_days, boll_k, begin_date, end_date, _collection_name_, datetime.now(), code),
                    flush=True
                )

                _log = open(
                    "/root/DataProcess/log/log_compute_"
                    + _collection_name_ + ".txt", 'r+'
                )
                content = _log.read()
                _log.seek(0, 0)
                _log.write(
                    'Error occurs when compute Boll_N%s_K%s signal from %s to %s, %s, %s, at position: %s \n \n' %
                    (boll_days, boll_k, begin_date, end_date, _collection_name_, datetime.now(), code) + content
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
            'Compute Boll_N%s_K%s signal from %s to %s, %s, %s, inserted: %4d, modified: %4d \n \n' %
            (boll_days, boll_k, begin_date, end_date, _collection_name_, datetime.now(),
             inserted_amount, updated_amount) + content
        )
        _log.flush()
        _log.close()


if __name__ == '__main__':
    BollSignal().compute('20050101', '20190409', 20, 2)
