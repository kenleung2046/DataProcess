import schedule
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
from datetime import datetime, timedelta
import time


def do_task():
    now_date = datetime.now()
    weekday = now_date.strftime('%w')

    current_date = now_date.strftime('%Y%m%d')

    crawl_calender(begin_date=current_date, end_date=current_date)

    if '0' < weekday < '6':

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


if __name__ == '__main__':
    schedule.every().day.at("19:00").do(do_task)

    while True:
        print(datetime.now(), flush=True)
        schedule.run_pending()
        time.sleep(30)
