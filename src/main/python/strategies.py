import time
import tushare as ts
import numpy as np
import pandas as pd
import argparse
from tqdm import tqdm
from stocks_info import StocksInfo
from utils import *

ts.set_token('63b8e92c576fdc9bcc8f4088f32673534e595a4622368ff397e2cac4')

pro = ts.pro_api()


class SentimentAnalysis(StocksInfo):

  def __init__(self,
               date_now=None,
               adj='qfq',
               period=31,
               today=False,
               batch_size=30,
               debug=False,
               use_tqdm=True,
               cb_mode='tushare',
               uqer_cb_dir=None):
    super(SentimentAnalysis, self).__init__(
        date_now=date_now,
        period=period,
        today=today,
        batch_size=batch_size,
        use_tqdm=use_tqdm,
        cb_mode=cb_mode,
        uqer_cb_dir=uqer_cb_dir
    )

    self.adj = adj

    if debug:
      self.ts_code_now = self.ts_code_now[:10]

  def _count_limit_ups_local(self, df_today, df_pre_day):
      up_total = 0
      down_total = 0
      limit_up_total = 0
      limit_down_total = 0
      limit_up_used_total = 0
      limit_up_ctn_total = 0
      limit_up_1_total = 0
      limit_up_2_total = 0
      limit_up_2_list = []
      limit_up_3_total = 0
      limit_up_3_list = []
      limit_up_more_total = 0
      limit_up_more_list = []

      df = pd.DataFrame(columns=(
        'ts_code', 'code', 'name', 'up_down_flag', 'limit_up', 'limit_down', 'limit_up_used',
        'limit_up_ctn', 'limit_up_count', 'up_ctn', 'up_count', 'up_count_true',
        'cb_code', 'cb_name', 'pct_chg(%)', 'pre_close_price',
        'open_price', 'close_price', 'high', 'low', 'volume', 'pct_vol(%)'
      ))
      if self.use_tqdm:
        iterator = tqdm(df_today.iterrows(), total=df_today.shape[0], ncols=100)
      else:
        iterator = df_today.iterrows()

      for code_i, row in iterator:

        if row['b']:
          continue

        name_i = row['name']
        if name_i is np.nan:
          name_i = ''
        new_flag = row['new']
        st_flag = row['st']
        sector = row['sector']
        open_price =  row['open']
        close_price =  row['close']
        high_price =  row['high']
        low_price =  row['low']
        pct_change = row['pct_chg_exact']

        pre_close_price = row['pre_close_qfq']

        up_down_flag = row['up_down']
        if up_down_flag == 1:
          up_total += 1
        if up_down_flag == -1:
          down_total += 1

        if new_flag:
          continue
        if sector == 'kcb':
          continue
        if st_flag:
          continue

        limit_up_flag = row['limit_up']
        limit_down_flag = row['limit_down']
        limit_up_used_flag = row['limit_up_used']
        limit_up_count_i = row['limit_up_count']

        if limit_up_flag:
          limit_up_total += 1

        if limit_up_used_flag:
          limit_up_used_total += 1

        if limit_down_flag:
          limit_down_total += 1

        if limit_up_count_i == 1:
         limit_up_1_total += 1

        limit_up_ctn_flag = row['limit_up_ctn']
        if limit_up_count_i > 1:
          limit_up_ctn_total += 1

        if limit_up_count_i == 2:
          limit_up_2_total += 1
          limit_up_2_list.append(name_i)

        if limit_up_count_i == 3:
          limit_up_3_total += 1
          limit_up_3_list.append(name_i)

        if limit_up_count_i > 3:
          limit_up_more_total += 1
          limit_up_more_list.append([name_i, limit_up_count_i])

        cb_code = row['cb_code']
        cb_name = row['cb_name']

        if code_i in df_pre_day.index:
          volume = row['vol']
          pre_volume = df_pre_day.loc[code_i, 'vol']
          vol_pct = round_exact((volume - pre_volume) * 100 / pre_volume, 2)
        else:
          volume = row['vol']
          vol_pct = np.nan

        up_ctn_flag = row['up_ctn']
        up_count_i = row['up_count']
        up_count_true_i = row['up_true_count']

        dict_i = {
          'ts_code': [code_i],
          'code': [code_i[:6]],
          'name': [name_i],
          'up_down_flag': [up_down_flag],
          'limit_up': [limit_up_flag],
          'limit_down': [limit_down_flag],
          'limit_up_used': [limit_up_used_flag],
          'limit_up_ctn': [limit_up_ctn_flag],
          'limit_up_count': [limit_up_count_i],
          'up_ctn': [up_ctn_flag],
          'up_count': [up_count_i],
          'up_count_true': [up_count_true_i],
          'cb_code': [cb_code],
          'cb_name': [cb_name],
          'pct_chg(%)': [pct_change],
          'pre_close_price': [pre_close_price],
          'open_price': [open_price],
          'close_price': [close_price],
          'high': [high_price],
          'low': [low_price],
          'volume': [volume],
          'pct_vol(%)': [vol_pct]
        }
        df = df.append(pd.DataFrame(dict_i), ignore_index=True)

      if limit_up_2_list:
        limit_up_2_list = ', '.join(limit_up_2_list)
      if limit_up_3_list:
        limit_up_3_list = ', '.join(limit_up_3_list)
      if limit_up_more_list:
        l_up_m_list = np.array(limit_up_more_list)
        l_up_m_list = l_up_m_list[l_up_m_list[:, 1].argsort()]
        limit_up_more_list = ', '.join(
            n_i + str(c_i) for n_i, c_i in l_up_m_list[::-1])

      df.set_index(['ts_code'], inplace=True)

      return df, up_total, down_total, limit_up_used_total, limit_down_total, \
             limit_up_total, limit_up_1_total, limit_up_ctn_total, \
             limit_up_2_total, limit_up_2_list, limit_up_3_total, \
             limit_up_3_list, limit_up_more_total, limit_up_more_list

  def _count_cvt_bond_up(self):
    df = pd.DataFrame(columns=(
      'cb_code', 'cb_name', 'stock_code', 'stock_name',
      'pct_chg(%)', 'pre_close_price', 'open_price', 'close_price', 'high', 'low', 'volume'
    ))

    for cb_code_i, (code_i, name_i) in self.bond2stock_dict.items():

      cb_name_i = self.cb_bond_name_dict[cb_code_i]

      df_i = self._get_realtime_quotes_safe(cb_code_i)

      open_price = float(df_i['open'][0])
      close_price = float(df_i['price'][0])
      high_price = float(df_i['high'][0])
      low_price = float(df_i['low'][0])
      volume = float(df_i['volume'][0])
      pre_close_price = float(df_i['pre_close'][0])

      pct_change = round_exact(
          (close_price - pre_close_price) * 100 / pre_close_price, 2)

      if close_price > pre_close_price:
        dict_i = {
          'cb_code': [cb_code_i],
          'cb_name': [cb_name_i],
          'stock_code': [code_i],
          'stock_name': [name_i],
          'pct_chg(%)': [pct_change],
          'pre_close_price': [pre_close_price],
          'open_price': [open_price],
          'close_price': [close_price],
          'high': [high_price],
          'low': [low_price],
          'volume': [volume]
        }
        df = df.append(pd.DataFrame(dict_i), ignore_index=True)

    return df

  def _count_csyx(self, threshold=0.05):
    df = pd.DataFrame(columns=(
      'code', 'name', 'pct_highest(%)', 'pct_chg(%)',
      'pre_close_price', 'open_price', 'close_price', 'high', 'low', 'volume'
    ))

    df_daily = safe_get(pro.daily, trade_date=self.date_now)

    for _, row in df_daily.iterrows():

      code_i = row['ts_code']

      if code_i[0] in ['9', '2']:
        continue

      name_i = self._stock_code_to_name(code_i)

      if 'ST' in name_i:
        continue

      close_price = row['close']
      high_price = row['high']
      volume = row['vol']

      if close_price < high_price * (1 - threshold):
        open_price = row['open']
        low_price = row['low']
        pre_close_price = row['pre_close']

        pct_change = round_exact(
            (close_price - pre_close_price) * 100 / pre_close_price, 2)
        pct = round_exact(
            (close_price - high_price) * 100 / high_price, 4)

        df = df.append(pd.DataFrame({
          'code': [code_i[:6]],
          'name': [name_i],
          'pct_highest(%)': [pct],
          'pct_chg(%)': [pct_change],
          'pre_close_price': [pre_close_price],
          'open_price': [open_price],
          'close_price': [close_price],
          'high': [high_price],
          'low': [low_price],
          'volume': [volume]
        }), ignore_index=True)

    return df.sort_values(by='pct_highest(%)', ascending=True)

  @staticmethod
  def _count_qssyx(df_today):
    def _calc_pct(_numerator, _denominator):
          return _numerator * 100 / _denominator

    df = pd.DataFrame(columns=(
        'code', 'name', 'pct_close(%)', 'amplitude', 'pct_high(%)', 'upper_shadow(%)',
        'close_price', 'open_price', 'high', 'low', 'pre_close_price', 'amount(E)', 'circ_mv(E)'
      ))

    for code_i, row in df_today.iterrows():
      name_i = row['name']
      if name_i is np.nan:
        name_i = ''
      open_price = row['open']
      close_price = row['close']
      high_price = row['high']
      low_price = row['low']
      pre_close_qfq = row['pre_close_qfq']
      amount = row['amount'] / 100000
      circ_mv = row['circ_mv'] / 10000

      high_pct = _calc_pct(high_price - pre_close_qfq, pre_close_qfq)
      case_1 = high_pct > 5
      syx_pct = _calc_pct(
          high_price - max(open_price, close_price), pre_close_qfq)
      case_2 = syx_pct > 3
      close_pct = _calc_pct(close_price - pre_close_qfq, pre_close_qfq)
      case_3 = close_pct > -4
      amplitude = _calc_pct(high_price - low_price, pre_close_qfq)
      case_4 = amplitude > 7
      case_5 = amount > 3

      if row['st'] or row['new'] or row['b'] or (row['sector'] == 'kcb'):
          continue

      if case_1 and case_2 and case_3 and case_4 and case_5:
          df = df.append(pd.DataFrame({
            'code': [code_i[:6]],
            'name': [name_i],
            'pct_close(%)': [round_exact(close_pct, 2)],
            'amplitude': [round_exact(amplitude, 2)],
            'pct_high(%)': [round_exact(high_pct, 2)],
            'upper_shadow(%)': [round_exact(syx_pct, 2)],
            'close_price': [close_price],
            'open_price': [open_price],
            'high': [high_price],
            'low': [low_price],
            'pre_close_price': [pre_close_qfq],
            'amount(E)': [round_exact(amount)],
            'circ_mv(E)': [round_exact(circ_mv)]
          }), ignore_index=True)

    df = df[[
        'code', 'name', 'pct_close(%)', 'amplitude', 'pct_high(%)', 'upper_shadow(%)',
        'close_price', 'open_price', 'high', 'low', 'pre_close_price', 'amount(E)', 'circ_mv(E)']]

    return df

  def _get_zs(self):
    szzs_close = round_exact(safe_get(
        pro.index_daily,
        ts_code='000001.SH',
        trade_date=self.date_now).loc[0, 'close'], 2)
    szzs_pre_close = round_exact(safe_get(
        pro.index_daily,
        ts_code='000001.SH',
        trade_date=self.date_now).loc[0, 'pre_close'], 2)
    szzs = round_exact(
        (szzs_close - szzs_pre_close) * 100 / szzs_pre_close, 2)

    szcz_close = round_exact(safe_get(
        pro.index_daily,
        ts_code='399001.SZ',
        trade_date=date).loc[0, 'close'], 2)
    szcz_pre_close = round_exact(safe_get(
        pro.index_daily,
        ts_code='399001.SZ',
        trade_date=date).loc[0, 'pre_close'], 2)
    szcz = round_exact(
        (szcz_close - szcz_pre_close) * 100 / szcz_pre_close, 2)

    cybz_close = safe_get(
        pro.index_daily,
        ts_code='399006.SZ',
        trade_date=self.date_now).loc[0, 'close']
    cybz_pre_close = safe_get(
        pro.index_daily,
        ts_code='399006.SZ',
        trade_date=self.date_now).loc[0, 'pre_close']
    cybz = round_exact(
        (cybz_close - cybz_pre_close) * 100 / cybz_pre_close, 2)

    return szzs, szcz, cybz

  def _save_info_excel(self, df, df_cb, df_csyx, df_qssyx, save_dir):
    info_path = join(save_dir, self.date)
    check_dirs([info_path])

    df_copy = df.copy()
    df = df.sort_values(
        by=['limit_up_count', 'pct_chg(%)', 'code'], ascending=[False, False, True])
    df = df[[
      'code', 'name', 'up_down_flag', 'limit_up', 'limit_down', 'limit_up_used',
      'limit_up_ctn', 'limit_up_count', 'up_ctn', 'up_count', 'up_count_true', 'cb_code', 'cb_name',
      'pct_chg(%)', 'pre_close_price', 'open_price', 'close_price', 'high', 'low', 'volume', 'pct_vol(%)',
      'MA5', 'pct_chg_5(%)', 'MA10', 'pct_chg_10(%)', 'MA20', 'pct_chg_20(%)',
      'MA30', 'pct_chg_30(%)', 'ma_up', 'ma_up_count'
    ]]
    df.to_csv(join(info_path, 'total.csv'), index=False)

    df_cb = df_cb[['cb_code', 'cb_name', 'stock_code', 'stock_name', 'pct_chg(%)',
                   'pre_close_price', 'open_price', 'close_price', 'high', 'low', 'volume']]
    df_cb = df_cb.sort_values(by=['pct_chg(%)', 'cb_code'], ascending=[False, True])
    df_cb.head(20).to_csv(join(info_path, 'premium_convert_bonds.csv'), index=False)

    df_csyx = df_csyx[['code', 'name', 'pct_highest(%)', 'pct_chg(%)', 'pre_close_price',
                       'open_price', 'close_price', 'high', 'low', 'volume']]
    df_csyx = df_csyx.sort_values(by=['pct_chg(%)', 'code'], ascending=[False, True])
    df_csyx.head(20).to_csv(join(info_path, 'long_upper_shadow.csv'), index=False)

    df_qssyx = df_qssyx[[
        'code', 'name', 'pct_close(%)', 'amplitude', 'pct_high(%)', 'upper_shadow(%)',
        'close_price', 'open_price', 'high', 'low', 'pre_close_price', 'amount(E)', 'circ_mv(E)']]
    df_qssyx = df_qssyx.sort_values(
        by=['pct_close(%)', 'code'], ascending=[False, True])
    df_qssyx.head(20).to_csv(join(info_path, 'trend_upper_shadow.csv'), index=False)

    df_bl = df_copy[df_copy['pct_vol(%)'] > 50]
    df_bl = df_bl[['code', 'name', 'pct_vol(%)', 'volume', 'pct_chg(%)',
                   'pre_close_price', 'open_price', 'close_price', 'high', 'low']]
    df_bl = df_bl.sort_values(
        by=['pct_vol(%)', 'pct_chg(%)', 'code'], ascending=[False, False, True])
    df_bl.head(20).to_csv(join(info_path, 'high_volume_open.csv'), index=False)

    df_ma = df_copy[df_copy['ma_up']]
    df_ma = df_ma[[
      'code', 'name', 'ma_up_count', 'pct_chg(%)',
      'MA2', 'pct_chg_2(%)', 'MA3', 'pct_chg_3(%)',
      'MA5', 'pct_chg_5(%)', 'MA10', 'pct_chg_10(%)',
      'MA20', 'pct_chg_20(%)', 'MA30', 'pct_chg_30(%)',
      'pre_close_price', 'open_price', 'close_price', 'high', 'low', 'volume']]
    df_ma = df_ma.sort_values(
        by=['ma_up_count', 'pct_chg(%)', 'code'], ascending=[False, False, True])
    df_ma.head(20).to_csv(join(info_path, 'ma_up.csv'), index=False)

  def update_daily(self, save_dir, dataset_dir):

    today_dataset_path = \
        join(join(dataset_dir, 'stocks'), self.date_now + '.p')
    pre_day_dataset_path = \
        join(join(dataset_dir, 'stocks'), self.pre_date + '.p')
    df_today = load_pkl(today_dataset_path)

    df_pre_day = load_pkl(pre_day_dataset_path)
    df, up_total, down_total, limit_up_used_total, \
        limit_down_total, limit_up_total, limit_up_1_total, \
        limit_up_ctn_total, limit_up_2_total, limit_up_2_list, \
        limit_up_3_total, limit_up_3_list, limit_up_more_total, \
        limit_up_more_list = self._count_limit_ups_local(df_today, df_pre_day)

    df_ma = df_today[[
      'ma_2', 'pct_chg_2', 'ma_3', 'pct_chg_3',
      'ma_5', 'pct_chg_5', 'ma_10', 'pct_chg_10', 'ma_20', 'pct_chg_20',
      'ma_30', 'pct_chg_30', 'ma_up', 'ma_up_count'
    ]]
    df_ma = df_ma.loc[df.index]
    df_ma.rename(columns={
      'ma_2': 'MA2', 'pct_chg_2': 'pct_chg_2(%)',
      'ma_3': 'MA3', 'pct_chg_3': 'pct_chg_3(%)',
      'ma_5': 'MA5', 'pct_chg_5': 'pct_chg_5(%)',
      'ma_10': 'MA10', 'pct_chg_10': 'pct_chg_10(%)',
      'ma_20': 'MA20', 'pct_chg_20': 'pct_chg_20(%)',
      'ma_30': 'MA30', 'pct_chg_30': 'pct_chg_30(%)',
      'ma_up': 'ma_up', 'ma_up_count': 'ma_up_count'}, inplace=True)
    df = pd.concat([df, df_ma], axis=1, sort=True)

    df_cb = self._count_cvt_bond_up()

    df_csys = self._count_csyx(threshold=0.05)

    df_qssyx = self._count_qssyx(df_today)

    # 保存limit_up_ctn信息表
    self._save_info_excel(df, df_cb, df_csys, df_qssyx, save_dir)

    # 指数
    szzs, szcz, cybz = self._get_zs()
    with open('./data/indices/indices_' + self.date + '.txt', 'w') as f:
        f.write('name, value\n')
        f.write('SH_index, {}\n'.format(szzs))
        f.write('SZ_index, {}\n'.format(szcz))
        f.write('GEB_index, {}\n'.format(cybz))

  def run(self, save_dir, dataset_dir):

    if self.date_now in self.trade_date:
      print('-' * 70)
      print('更新市场情绪表...')
      self.update_daily(save_dir, dataset_dir)
    else:
      print('-' * 70)
      print('非交易日！')
      self._save_sentiment_excel_null(save_dir)


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description=".")
  parser.add_argument('-d', '--date', action="store_true",
                      help="date")

  if args.date:
      time_start = time.time()
      SentimentAnalysis(
          date_now=date,
          period=31,
          today=False,
          debug=False,
          cb_mode='tushare'
      ).run(save_dir='data/scanner', dataset_dir='data/daily_source')
  else:
      time_start = time.time()
      SentimentAnalysis(
          date_now='20191126',
          period=31,
          today=False,
          debug=False,
          cb_mode='tushare'
      ).run(save_dir='data/scanner', dataset_dir='data/daily_source')
