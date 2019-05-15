import schedule
from pymongo import MongoClient
from calendar_crawler import crawl_calender
from basic_crawler import crawl_basic
from name_crawler import name_his_crawler
from suspension_crawler import crawl_suspension
from index_crawler import crawl_index_daily
from quotation_crawler import crawl_quotation_daily
from quotation_fixing import fill_is_trading, fill_suspension_data
from fundamental_crawler import crawl_fundamental_daily
from adj_factor_compute import adj_factor_compute
from boll_signal_capture import BollSignal
from ma_signal_capture import get_ma_signal
from quantification import atr_compute_daily
from datetime import datetime, timedelta
import time


_database_ip_ = '127.0.0.1'
_database_port_ = 27017
_authentication_ = 'A-Shares'
_user_ = 'manager'
_pwd_ = 'Kl!2#4%6'
_database_name_ = 'A-Shares'
_collection_name_ = 'calendar'
_client = MongoClient(_database_ip_, _database_port_)
db_auth = _client[_authentication_]
db_auth.authenticate(_user_, _pwd_)
db = _client[_database_name_]


def do_task():
    now_date = datetime.now()

    current_date = now_date.strftime('%Y%m%d')

    crawl_calender(begin_date=current_date, end_date=current_date)
    time.sleep(30)

    market_open = db.calendar.find_one({'cal_date': current_date, 'is_open': True})

    if market_open is not None:

        crawl_basic(update_date=current_date)
        time.sleep(3)
        crawl_basic(list_status='D', update_date=current_date)
        time.sleep(3)
        crawl_basic(list_status='P', update_date=current_date)
        time.sleep(3)

        name_his_crawler(begin_date=current_date, end_date=current_date)
        time.sleep(3)

        crawl_suspension(begin_date=current_date, end_date=current_date)
        time.sleep(3)

        crawl_index_daily(begin_date=current_date, end_date=current_date)
        time.sleep(3)

        crawl_quotation_daily(begin_date=current_date, end_date=current_date, adj=None)
        time.sleep(70)
        crawl_quotation_daily(begin_date=current_date, end_date=current_date, adj='hfq')
        time.sleep(70)
        crawl_quotation_daily(begin_date=current_date, end_date=current_date, adj='qfq')
        time.sleep(3)

        fill_is_trading(begin_date=current_date, end_date=current_date, adj=None)
        time.sleep(3)
        fill_is_trading(begin_date=current_date, end_date=current_date, adj='hfq')
        time.sleep(3)
        fill_is_trading(begin_date=current_date, end_date=current_date, adj='qfq')
        time.sleep(3)

        fill_suspension_data(begin_date='20151201', end_date=current_date, adj=None)
        time.sleep(3)
        fill_suspension_data(begin_date='20151201', end_date=current_date, adj='hfq')
        time.sleep(3)
        fill_suspension_data(begin_date='20151201', end_date=current_date, adj='qfq')

        crawl_fundamental_daily(begin_date=current_date, end_date=current_date)
        time.sleep(3)

        adj_factor_compute(begin_date=current_date, end_date=current_date)
        time.sleep(3)

        before_one_and_half_year = (now_date - timedelta(days=540)).strftime('%Y%m%d')

        BollSignal().compute(begin_date=before_one_and_half_year, end_date=current_date, boll_days=20, boll_k=2)
        time.sleep(3)

        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=5)
        time.sleep(3)
        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=10)
        time.sleep(3)
        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=20)
        time.sleep(3)
        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=30)
        time.sleep(3)
        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=60)
        time.sleep(3)
        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=120)
        time.sleep(3)
        get_ma_signal(begin_date=before_one_and_half_year, end_date=current_date, ma_days=240)
        time.sleep(3)

        atr_compute_daily(current_date=current_date)

    else:
        pass


if __name__ == '__main__':
    schedule.every().day.at("19:00").do(do_task)

    while True:
        print(datetime.now(), flush=True)
        schedule.run_pending()
        time.sleep(30)
