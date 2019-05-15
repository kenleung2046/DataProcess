from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
import tushare as ts

_token_ = '52b3d356995423a9f694a1bb792f789386d2e88f02d5b47bbf5a7e22'
pro = ts.pro_api(_token_)

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


def fill_is_trading(begin_date, end_date, adj=None):
    calendar_cursor = db.calendar.find(
        {
            'cal_date': {'$gte': begin_date, '$lte': end_date},
            'is_open': True
        },
        sort=[('cal_date', ASCENDING)],
        projection={'cal_date': True, '_id': False},
    )
    dates = [x['cal_date'] for x in calendar_cursor]

    updated_amount = 0

    _collection_name_ = 'quotation_daily' if adj is None else 'quotation_daily_' + adj

    for date in dates:
        quotation_cursor = db[_collection_name_].find(
            {'trade_date': date},
            projection={'ts_code': True, 'vol': True, '_id': False},
            batch_size=1000
        )

        update_requests = []
        for quotation in quotation_cursor:
            is_trading = (quotation['vol'] > 0)

            update_requests.append(
                UpdateOne(
                    {'ts_code': quotation['ts_code'],
                     'trade_date': date},
                    {'$set': {'is_trading': is_trading}}
                )
            )

        if len(update_requests) > 0:
            update_result = db[_collection_name_].bulk_write(
                update_requests,
                ordered=False
            )

            print(
                'Filled "is_trading" at date: %s, %s-%s, %s, modified: %4d' %
                (date, _database_name_, _collection_name_, datetime.now(),
                 update_result.modified_count),
                flush=True
            )

            updated_amount = updated_amount + update_result.modified_count

    print('total modified amount is %s' % updated_amount)
    _log = open(
        "/root/DataProcess/log/log_fill_data_"
        + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
    )
    content = _log.read()
    _log.seek(0, 0)
    _log.write(
        'Filled "is_trading" from %s to %s, %s-%s, %s, modified: %4d \n \n' %
        (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
         updated_amount) + content
    )
    _log.flush()
    _log.close()


def fill_suspension_data(begin_date, end_date, adj=None):
    basic_cursor = db.stock_basic.find(
        {},
        sort=[('ts_code', ASCENDING)],
        projection={'ts_code': True, 'list_date': True, '_id': False}
    )
    basics = [basic for basic in basic_cursor]

    calendar_cursor = db.calendar.find(
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

    _collection_name_ = 'quotation_daily' if adj is None else 'quotation_daily_' + adj

    code_last_trading_close_dict = dict()
    for date in dates:
        last_quotation_code_set = set(code_last_trading_close_dict.keys())

        update_requests = []
        for basic in basics:

            code = basic['ts_code']

            if date < basic['list_date']:
                pass

            else:
                quotation = db[_collection_name_].find_one(
                    {'ts_code': code,
                     'trade_date': date},
                    projection={'close': True, 'vol': True, '_id': False}
                )
                if quotation is not None and quotation['vol'] > 0:
                    code_last_trading_close_dict[code] = quotation['close']

                    last_quotation_code_set.add(code)

                else:
                    if code in last_quotation_code_set:
                        last_trading_close = code_last_trading_close_dict[code]
                        document = {
                            'ts_code': code,
                            'trade_date': date,
                            'is_trading': False,
                            'vol': 0,
                            'amount': 0,
                            'change': 0,
                            'pct_chg': 0,
                            'open': last_trading_close,
                            'close': last_trading_close,
                            'low': last_trading_close,
                            'high': last_trading_close,
                            'pre_close': last_trading_close,
                        }

                        update_requests.append(
                            UpdateOne(
                                {'ts_code': code,
                                 'trade_date': date},
                                {'$set': document},
                                upsert=True
                            ))

        if len(update_requests) > 0:
            update_result = db[_collection_name_].bulk_write(
                update_requests,
                ordered=False
            )

            print(
                'Filled suspend stock with quotation data at date: %s, %s-%s, %s, inserted: %4d, modified: %4d' %
                (date, _database_name_, _collection_name_, datetime.now(),
                 update_result.upserted_count, update_result.modified_count),
                flush=True
            )

            inserted_amount = inserted_amount + update_result.upserted_count
            updated_amount = updated_amount + update_result.modified_count

    print('total inserted amount is %s, total modified amount is %s' % (inserted_amount, updated_amount))
    _log = open(
        "/root/DataProcess/log/log_fill_data_"
        + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
    )
    content = _log.read()
    _log.seek(0, 0)
    _log.write(
        'Filled suspend stock with quotation data from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
        (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
         inserted_amount, updated_amount) + content
    )
    _log.flush()
    _log.close()


# def fill_high_limit_low_limit(begin_date, end_date, adj=None):
#     df_new_stocks = pro.new_share()
#
#     code_ipo_date_set = set()
#     code_ipo_price_dict = dict()
#
#     for index in df_new_stocks.index:
#         ipo_price = df_new_stocks.loc[index]['price']
#         code = df_new_stocks.loc[index]['ts_code']
#         ipo_date = df_new_stocks.loc[index]['ipo_date']
#
#         code_ipo_price_dict[code + '_' + ipo_date] = ipo_price
#         code_ipo_date_set.add(code + '_' + ipo_date)
#
#     basic_cursor = db.Stock_Basic.find(
#         {'list_status': 'L',
#          'list_date': {'$lte': end_date}},
#         sort=[('ts_code', ASCENDING)],
#         projection={'_id': False, 'ts_code': True}
#     )
#     # codes = [x['ts_code'] for x in basic_cursor]
#
#     codes = ['000001.SZ', '000002.SZ']
#
#     name_cursor = db.Name_His.find(
#         {'start_date': {'$gte': begin_date, '$lte': end_date}},
#         projection={'ts_code': True, 'start_date': True, 'name': True, '_id': False}
#     )
#     code_date_name_dict = dict([(y['ts_code'] + '_' + y['start_date'], y['name']) for y in name_cursor])
#     code_date_keys_set = set(code_date_name_dict.keys())
#
#     inserted_amount = 0
#     updated_amount = 0
#
#     _collection_name_ = 'Quotation_Daily' if adj is None else 'Quotation_Daily_' + adj
#
#     for code in codes:
#         quotation_cursor = db.Quotation_Daily.find(
#             {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}},
#             sort=[('trade_date', ASCENDING)],
#             projection={'trade_date': True, 'pre_close': True, '_id': False}
#         )
#
#         update_requests = []
#
#         for quotation in quotation_cursor:
#             trade_date = quotation['trade_date']
#             code_date_key = code + '_' + trade_date
#
#             high_limit = -1
#             low_limit = -1
#
#             pre_close = quotation['pre_close']
#
#             if code_date_key in code_ipo_date_set:
#                 high_limit = code_ipo_price_dict[code_date_key] * 1.44
#                 low_limit = code_ipo_price_dict[code_date_key] * 0.64
#                 document = {
#                     'high_limit': high_limit,
#                     'low_limit': low_limit
#                 }
#                 update_requests.append(
#                     UpdateOne(
#                         {'ts_code': code,
#                          'trade_date': trade_date},
#                         {'$set': document}
#                     )
#                 )
#             elif code_date_key in code_date_keys_set and code_date_name_dict[code_date_key][0:2] in ['ST', '*S'] \
#                     and pre_close > 0:
#                 high_limit = pre_close * 1.05
#                 low_limit = pre_close * 0.95
#                 document = {
#                     'high_limit': high_limit,
#                     'low_limit': low_limit
#                 }
#                 update_requests.append(
#                     UpdateOne(
#                         {'ts_code': code,
#                          'trade_date': trade_date},
#                         {'$set': document}
#                     )
#                 )
#             elif pre_close > 0:
#                 high_limit = pre_close * 1.1
#                 low_limit = pre_close * 0.9
#                 document = {
#                     'high_limit': high_limit,
#                     'low_limit': low_limit
#                 }
#                 update_requests.append(
#                     UpdateOne(
#                         {'ts_code': code,
#                          'trade_date': trade_date},
#                         {'$set': document}
#                     )
#                 )
#
#         if len(update_requests) > 0:
#             update_result = db[_collection_name_].bulk_write(
#                 update_requests,
#                 ordered=False
#             )
#
#             print(
#                 'Filled stock high limit and low limit data at code: %s, %s-%s, %s, inserted: %4d, modified: %4d' %
#                 (code, _database_name_, _collection_name_, datetime.now(),
#                  update_result.upserted_count, update_result.modified_count),
#                 flush=True
#             )
#
#             inserted_amount = inserted_amount + update_result.upserted_count
#             updated_amount = updated_amount + update_result.modified_count
#
#     print('total inserted amount is %s, total modified amount is %s' % (inserted_amount, updated_amount))
#     _log = open(
#         "/Users/Kenny2046/Axe_Capital/log/log_fill_data_"
#         + _database_name_ + "_" + _collection_name_ + ".txt", 'r+'
#     )
#     content = _log.read()
#     _log.seek(0, 0)
#     _log.write(
#         'Filled stock high limit and low limit data from %s to %s, %s-%s, %s, inserted: %4d, modified: %4d \n \n' %
#         (begin_date, end_date, _database_name_, _collection_name_, datetime.now(),
#          inserted_amount, updated_amount) + content
#     )
#     _log.flush()
#     _log.close()


if __name__ == '__main__':
    fill_is_trading('20190306', '20190306')
    # fill_is_trading('20190306', '20190306', adj='hfq')
    # fill_is_trading('20190306', '20190306', adj='qfq')
    # fill_suspension_data('20151201', '20190306')
    # fill_suspension_data('20151201', '20190306', adj='hfq')
    # fill_suspension_data('20151201', '20190306', adj='qfq')
    # fill_high_limit_low_limit('20190306', '20190306')
    # fill_high_limit_low_limit('20151201', '20190306', adj='hfq')
    # fill_high_limit_low_limit('20151201', '20190306', adj='qfq')
