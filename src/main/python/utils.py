import os
import pickle
import socket
from decimal import Decimal
from os.path import isdir, join


def check_dirs(dir_list):
  for dir_path in dir_list:
    if not isdir(dir_path):
      os.makedirs(dir_path)

def round_exact(value, dec_digits=2):
  result = str(value).strip()
  if result != '':
    zero_count = dec_digits
    index_dec = result.find('.')
    if index_dec > 0:
      zero_count = len(result[index_dec + 1:])
      if zero_count > dec_digits:
        if int(result[index_dec + dec_digits + 1]) > 4:
          if value < 0:
            result = str(Decimal(result[:index_dec + dec_digits + 1])
                         - Decimal(str(pow(10, dec_digits * -1))))
          else:
            result = str(Decimal(result[:index_dec + dec_digits + 1])
                         + Decimal(str(pow(10, dec_digits * -1))))
        index_dec = result.find('.')
        result = result[:index_dec + dec_digits + 1]
        zero_count = 0
      else:
        zero_count = dec_digits - zero_count
    else:
      result += '.'
    for i in range(zero_count):
      result += '0'
  return float(result)

def save_df(df, file_dir, file_name, save_format, verbose=True, index=True):
  check_dirs([file_dir])
  if save_format == 'pickle':
    file_path = join(file_dir, file_name + '.p')
    if verbose:
      print('Save {} ...'.format(file_path))
    df.to_pickle(file_path)
  elif save_format == 'csv':
    file_path = join(file_dir, file_name + '.csv')
    if verbose:
      print('Save {} ...'.format(file_path))
    df.to_csv(file_path, index=index)
  elif save_format == 'excel':
    file_path = join(file_dir, file_name + '.xlsx')
    if verbose:
      print('Save {} ...'.format(file_path))
    df.to_excel(file_path)
  else:
    raise ValueError

def load_pkl(data_path, verbose=True):
  with open(data_path, 'rb') as f:
    if verbose:
    return pickle.load(f)


def safe_get(func, *args, **kwargs):
  result = None
  while result is None:
    try:
      result = func(*args, **kwargs)
    except socket.error:
      print('Time Out!')
    except KeyboardInterrupt:
      exit()
  return result
