import json
import os

import toolkit_config as cfg

ROOTDIR = os.path.join(cfg.PRJDIR,'project1')
DATDIR = os.path.join(ROOTDIR,'data')
TICPATH = os.path.join(ROOTDIR, 'TICKERS.txt')

COLUMNS = ['Volume', 'Date', 'Adj Close', 'Close', 'Open', 'High']
COLWIDTHS = {'Volume': 14, 'Date': 11, 'Adj Close': 19, 'Close': 10, 'Open': 6, 'High': 20}

def get_tics(pth):
    """ Reads a file containing tickers and their corresponding exchanges.
    Each non-empty line of the file is guaranteed to have the following format:

    "XXXX"="YYYY"

    where:
        - XXXX represents an exchange.
        - YYYY represents a ticker.

    This function should return a dictionary, where each key is a properly formatted
    ticker, and each value the properly formatted exchange corresponding to the ticker.

    Parameters
    ----------
    pth : str
        Full path to the location of the TICKERS.txt file.

    Returns
    -------
    dict
        A dictionary with format {<tic> : <exchange>} where
            - Each key (<tic>) is a ticker found in the file specified by pth (as a string).
            - Each value (<exchange>) is a string containing the exchange for this ticker.
    """
    tic_map = {}

    with open(pth,mode='rt',encoding='utf-8') as file:
        for line in file:
            part = line.strip().split("=")
            if len(part) >=2:
                ticker_code = part[-1].strip('"').lower()
                ticker_exchange = part[0].strip('"').lower()
                tic_map[ticker_code] = ticker_exchange
    return tic_map


def read_dat(tic):
    file_path = os.path.join(DATDIR,f'{tic}_prc.dat')
    data = []
    with open(file_path,mode='rt',encoding='utf-8') as file:
        for line in file:
            data.append(line.strip())

    return data

def line_to_dict(line):
    """Returns the information contained in a line of a ".dat" file as a
      dictionary, where each key is a column name and each value is a string
      with the value for that column.

      This line will be split according to the field width in `COLWIDTHS`
      of each column in `COLUMNS`.

      Parameters
      ----------
      line : str
          A line from ".dat" file, without any newline characters

      Returns
      -------
      dict
          A dictionary with format {<col> : <value>} where
          - Each key (<col>) is a column in `COLUMNS` (as a string)
          - Each value (<value>) is a string containing the correct value for
            this column.
    """
    result = {}
    start = 0

    for col in COLUMNS:
        width = COLWIDTHS[col]
        result[col] = line[start: start+width]
        start += width

    return result

def verify_tickers(tic_exchange_dic, tickers_lst=None):
    if tickers_lst is not None:
        if len(tickers_lst) > 0:
            for i in tickers_lst:
                if i in tic_exchange_dic:
                    continue
                else:
                    raise Exception
        else:
            raise Exception

def verif_cols(col_lst=None):
    if col_lst is not None:
        if len(col_lst) > 0:
            for i in col_lst:
                if i in COLUMNS:
                    continue
                else:
                    raise Exception
        else:
            raise Exception

def create_data_dict(tic_exchange_dic, tickers_lst=None, col_lst=None):
    """Returns a dictionary containing the data for the tickers specified in tickers_lst.
        An Exception is raised if any of the tickers provided in tickers_lst or any of the
        column names provided in col_lst are invalid.

        Parameters
        ----------
        tic_exchange_dic: dict
            A dictionary returned by the `get_tics` function

        tickers_lst : list, optional
            A list containing tickers (as strings)

        col_lst : list, optional
            A list containing column names (as strings)

        Returns
        -------
        dict
            A dictionary with format {<tic> : <data>} where
            - Each key (<tic>) is a ticker in tickers_lst (as a string)
            - Each value (<data>) is a dictionary with format
                {
                    'exchange': <tic_exchange>,
                    'data': [<dict_0>, <dict_1>, ..., <dict_n>]
                }
              where
                - <tic_exchange> refers to the exchange that <tic> belongs to in lower case.
                - <dict_0> refers to the dictionary returned by line_to_dict(read_dat(<tic>)[0]),
                  but that only contains the columns listed in col_lst
                - <dict_n> refers to the dictionary returned by line_to_dict(read_dat(<tic>)[-1]),
                  but that only contains the columns listed in col_lst
    """
    if not col_lst:
        col_lst = None
    if col_lst:
        verif_cols(col_lst)

    target_ticker = tickers_lst or list(tic_exchange_dic.keys())
    if tickers_lst:
        verify_tickers(tic_exchange_dic, tickers_lst)

    # Process the data of each stock code
    def process_ticker_data(ticker):
        raw_data = read_dat(ticker)
        processed_data = []
        for line in raw_data:
            # If a column name is specified, only the specified column will be retained.
            # Otherwise, retain all columns
            raw_line = line_to_dict(line)
            filter_ticker = (
                {key: raw_line[key] for key in col_lst if key in raw_line}
                if col_lst else raw_line
            )
            processed_data.append(filter_ticker)

        return {
            'exchange': tic_exchange_dic[ticker].lower(),
            'data': processed_data
        }

    return {ticker: process_ticker_data(ticker) for ticker in target_ticker}


def create_json(data_dict, pth):
    """Saves the data found in the data_dict dictionary into a
        JSON file whose name is specified by pth.

        Parameters
        ----------
        data_dict: dict
            A dictionary returned by the `create_data_dict` function

        pth : str
            The complete path to the output JSON file. This is where the file with
            the data will be saved.


        Returns
        -------
        None
            This function does not return anything

    """
    with open(pth,'wt',encoding='utf-8') as file:
        json.dump(data_dict,file,ensure_ascii=False,indent=4)


def _test_get_tics():
    pth = TICPATH
    result = get_tics(pth)
    print(result)

def _test_read_dat():
    pth = TICPATH
    tics = sorted(list(get_tics(pth).keys()))
    tic = tics[0]
    lines = read_dat(tic)
    print(lines[0])

def _test_line_to_dict():
    pth = TICPATH
    tics = sorted(list(get_tics(pth).keys()))
    lines = read_dat(tics[0])
    dic = line_to_dict(lines[0])
    print(dic)

def _test_create_data_dict():
    pth = TICPATH
    tic_exchange_dic = get_tics(pth)
    tickers_lst = ['aapl', 'baba']
    col_lst = ['Date', 'Close']
    data_dict = create_data_dict(tic_exchange_dic, tickers_lst, col_lst)

    for tic in tickers_lst:
        data_dict[tic]['data'] = data_dict[tic]['data'][:3]

    print(data_dict)

def _test_create_json(json_pth):
    """ Test function for the `create_json_ function.
    This function will save the dictionary returned by `create_data_dict` to the path specified.

    """
    pth = TICPATH
    tic_exchange_dic = get_tics(pth)
    tickers_lst = ['aapl', 'baba']
    col_lst = ['Date', 'Close']
    data_dict = create_data_dict(tic_exchange_dic, tickers_lst, col_lst)
    create_json(data_dict, json_pth)
    print(f'Data saved to {json_pth}')

if __name__ == '__main__':
    # _test_get_tics()
    # _test_read_dat()
    # _test_line_to_dict()
    # _test_create_data_dict()
    _test_create_json(os.path.join(DATDIR,'data.json'))
    pass
