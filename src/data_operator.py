import pandas as pd

class data_operator:
    def __init__(self, client, pair, interval, start_timestamp=None, end_timestamp=None, start_open_time=None, end_open_time=None):
        
        # alap attributumok
        self.client   = client
        self.pair     = pair
        self.interval = interval

        # Ha meg vannak adva a timestamp-ek, az open_time-okat számoljuk ki
        if start_timestamp is not None and end_timestamp is not None:
            self.start_timestamp = start_timestamp
            self.end_timestamp   = end_timestamp
            self.start_open_time = pd.to_datetime(start_timestamp / 1000, unit='s')
            self.end_open_time   = pd.to_datetime(end_timestamp   / 1000, unit='s')

        # Ha a dátumok (open_time-ok) vannak megadva stringként, a timestamp-eket számoljuk ki
        elif start_open_time is not None and end_open_time is not None:
            # Convert string dates to datetime
            self.start_open_time = pd.to_datetime(start_open_time)
            self.end_open_time   = pd.to_datetime(end_open_time)
            # Convert datetime to timestamps
            self.start_timestamp = int(self.start_open_time.timestamp() * 1000)
            self.end_timestamp   = int(self.end_open_time.timestamp()   * 1000)

        else:
            raise ValueError("Either timestamps or open times must be provided.")

        # Inicializáljuk a raw_data-t üres DataFrame-ként
        self.raw_data = pd.DataFrame()

    def __repr__(self):
        return (f"data_operator(\n"
                f"  pair={self.pair},\n"
                f"  interval={self.interval},\n"
                f"  start_timestamp={self.start_timestamp},\n"
                f"  end_timestamp={self.end_timestamp},\n"
                f"  start_open_time={self.start_open_time},\n"
                f"  end_open_time={self.end_open_time}\n"
                f")")

    def fetch_data(self):
        # Lekérdezzük az adatokat a Binance API-ról
        df_temp = self.client.get_historical_klines(self.pair, self.interval, self.start_timestamp, self.end_timestamp)

        # Formázzuk az adatokat
        df_temp = pd.DataFrame(df_temp).iloc[:, :6]
        df_temp.columns = ["open_time", "open", "high", "low", "close", "volume"]
        df_temp = df_temp.astype(
            {"open_time": "int64", "open": "float", "high": "float", "low": "float", "close": "float", "volume": "float"}
        )

        # open_date_time oszlop hozzáadása az open_time alapján, 2 órás eltolással
        delta = pd.Timedelta(hours=1)
        df_temp['open_date_time'] = (pd.to_datetime(df_temp['open_time'] / 1000, unit='s') + delta).dt.strftime('%Y-%m-%d %H:%M')
        df_temp.insert(1, 'open_date_time', df_temp.pop('open_date_time'))


        # Az adatokat open_time szerint csökkenő sorrendbe rendezzük
        df_temp = df_temp.sort_values('open_time', ascending=False)

        # Tároljuk a raw_data attribútumban
        self.raw_data = df_temp


## example calling ##

