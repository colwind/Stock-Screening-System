import numpy as np
import pandas as pd
import tushare as ts
from pandas import datetime as dt
from utils import *

ts.set_token('63b8e92c576fdc9bcc8f4088f32673534e595a4622368ff397e2cac4')
pro=ts.pro_api()

class StocksInfo(object):

  def __init__(self,
               date_now=None,
               period=20,
               today=False,
               batch_size=30,
               use_tqdm=True):

    self.start_date = '20100104'
    if date_now:
      self.date_now = date_now
      self.today = False
    else:
      self.date_now = dt.now().strftime('%Y%m%d')
      self.today = True

    if today:
      self.today = True

    self.period = period

    self.batch_size = batch_size

    self.trade_date: np.ndarray = self._get_trade_date()
    self.trade_date_back: np.ndarray = self.trade_date[::-1]
    self.pre_date = self.trade_date[-2]

    self.ts_code_now = pro.daily(trade_date=self.date_now)['ts_code'].values

    self.stock_name_dict: dict = self._get_name_dict()

    self.stock2bond_dict, self.bond2stock_dict, \
        self.cb_stock_list, self.cb_bond_list, \
        self.cb_bond_name_dict = self._load_cvt_bond_tushare()

    self.use_tqdm = use_tqdm

  def _get_trade_date(self):
    df_sse = safe_get(
        pro.trade_cal, exchange='SSE', start_date=self.start_date,
        end_date=self.date_now, is_open='1')
    df_szse = safe_get(
        pro.trade_cal, exchange='SZSE', start_date=self.start_date,
        end_date=self.date_now, is_open='1')
    date_sse = df_sse['cal_date'].values
    date_szse = df_szse['cal_date'].values
    date = np.union1d(date_sse, date_szse)
    self.start_date = date[-self.period]
    return date

  @staticmethod
  def _get_name_dict():
    df_list = []
    for list_status in ['L', 'D', 'P']:
      df_i = safe_get(
          pro.stock_basic,
          exchange='',
          list_status=list_status,
          fields='ts_code, name')
      df_list.append(df_i)
    df = pd.concat(df_list)

    stock_name_dict = {}
    for _, info_row in df.iterrows():
      stock_name_dict[info_row['ts_code'][:6]] = info_row['name']
    return stock_name_dict

  def _stock_code_to_name(self, code):
    try:
      name = self.stock_name_dict[code[:6]]
    except KeyError:
      name = ''
    return name

  def _load_cvt_bond_tushare(self):
    df_cb_all = safe_get(pro.cb_basic)
    df_cb_all = df_cb_all.drop_duplicates(subset='ts_code')
    cb_ts_code_list = df_cb_all['ts_code'].astype('str')
    cb_code_list = [code_i[:6] for code_i in cb_ts_code_list]
    df_cb_all['ts_code'] = cb_code_list

    cb_list_all = set(df_cb_all['ts_code'].values)
    df_cb = self._get_realtime_quotes_safe(cb_list_all)
    df_cb = df_cb[df_cb['price'] != 0]
    df_cb.set_index('code', inplace=True)
    df_cb.index.name = 'bcode'

    df_cb_all.set_index('ts_code', inplace=True)
    df = df_cb_all.loc[df_cb.index]
    df.reset_index(inplace=True)

    cb_stock_list = []
    cb_bond_list = []
    stock2bond_dict = {}
    bond2stock_dict = {}
    cb_bond_name_dict = {}
    for _, row_i in df.iterrows():
      stock_code = str(row_i['stk_code']).zfill(6)[:6]
      stock_name = row_i['stk_short_name']
      bond_code = str(row_i['ts_code']).zfill(6)[:6]
      bond_name = row_i['bond_short_name']

      if 'EB' in bond_name:
        continue

      cb_stock_list.append(stock_code)
      cb_bond_list.append(bond_code)
      stock2bond_dict[stock_code] = (bond_code, bond_name)
      bond2stock_dict[bond_code] = (stock_code, stock_name)
      cb_bond_name_dict[bond_code] = bond_name

    return stock2bond_dict, bond2stock_dict, \
           cb_stock_list, cb_bond_list, cb_bond_name_dict

  @staticmethod
  def _get_realtime_quotes_safe(code_list):
    if type(code_list) in [list, set, np.ndarray]:
      code_list = list(code_list)
      if len(list(code_list)[0]) != 6:
        code_list = [code[:6] for code in code_list]
    else:
      if len(code_list) != 6:
        code_list = code_list[:6]
    return safe_get(ts.get_realtime_quotes, code_list)

  def _get_data_batch_generator(self, code_list, batch_size=None):
    for start in range(0, len(code_list), batch_size):
      end = start + batch_size
      code_list_batch = code_list[start:end]
      data_batch = self._get_realtime_quotes_safe(code_list_batch)
      yield code_list_batch, data_batch

  def _get_data_batch(self, code_list, i_batch):
    code_list_batch = code_list[i_batch * self.batch_size:
                                (i_batch + 1) * self.batch_size]
    data_batch = self._get_realtime_quotes_safe(code_list_batch)
    return code_list_batch, data_batch

  @staticmethod
  def _get_real_time_batch(i, code_list_batch, data_batch):

    code = code_list_batch[i]
    df = data_batch[data_batch['code'] == code]
    name = df['name'][i]
    sell1_v = df['a1_v'][i]
    sell1_v = 0 if sell1_v == '' else int(sell1_v)
    buy1_v = df['b1_v'][i]
    buy1_v = 0 if buy1_v == '' else int(buy1_v)

    price = float(df['price'][i])
    pre_close = float(df['pre_close'][i])
    pct_change = ((price - pre_close) / pre_close) * 100

    return code, name, price, pre_close, sell1_v, buy1_v, pct_change

  @staticmethod
  def _get_sector(code):
    if code[:3] == '688':
      return 'kcb'
    elif code[0] == '6':
      return 'sh'
    elif code[0] == '0':
      return 'sz'
    elif code[0] == '3':
      return 'cyb'
    else:
      return ''
