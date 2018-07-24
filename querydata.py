from sqlalchemy.orm import Load, load_only

class QueryDataAdapter(object):
    def __init__(self, query, session=None, columns=None, fetch_by=100):
        """
        :param query: sqlalchemy query
        :param session: sqlalchemy session
        :param columnss: columns to handle
        :param fetch_by: number of rows to fetch
        """
        self._s = session
        self.q = query

        self.fetch_size = fetch_by

        self.row_count = self.q.count()
        self.current_count = min(self.fetch_size, self.row_count)
        self.data = self.q.slice(0, self.current_count).all()
        self.columns = []
        cd = self.q.column_descriptions
        if len(self.q.column_descriptions) > 1:
            for d in self.q.column_descriptions:
                if hasattr(d['expr'], '__table__'):
                    self.columns.extend(
                        map(lambda x: "{0}.{1}".format(d['name'], x), d['expr'].__table__.columns.keys()))
                else:
                    self.columns.append(d['name'])
        else:
            self.columns.extend(cd[0]['expr'].__table__.columns.keys())

        if columns:
            self.columns = [x for x in columns if x in self.columns]

    def get(self, row, col):
        query_data_row = self.__getitem__(row)
        column = self.columns[col]
        if '.' in column:
            table_name, column_name = column.split('.')

            table = getattr(query_data_row, table_name)
            data = getattr(table, column_name)
            return data
        else:
            data = getattr(query_data_row, column)
            return data

    def set(self, row, col, value):
        pass

    def fetch(self):
        to_fetch = min(self.fetch_size, self.row_count - self.current_count)
        if to_fetch <= 0:
            return False
        fetched = self.current_count + to_fetch
        data = self.q.slice(self.current_count, fetched).all()
        self.data.extend(data)
        self.current_count = fetched
        return True

    def __getitem__(self, item):
        if item > self.row_count:
            raise IndexError
        if item < self.current_count:
            return self.data[item]
        else:
            self.fetch()
            self.__getitem__(item)

    def __setitem__(self, key, value):
            self._s.add(value)
            self._s.flush()
            self.data.insert(key,value)