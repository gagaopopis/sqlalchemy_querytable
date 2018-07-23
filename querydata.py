from sqlalchemy.orm import Load

class QueryTable(object):
    def __init__(self, query=None, sql_table=None, session=None, only_cols=None, header=None, child=None, fetch_by=100, order_by=None):
        self.child = child
        if sql_table and session:
            self.q = session.query(sql_table)
        elif query:
            self.q = query
        else:
            return

        if only_cols:
            self.q = self.q.options(Load(sql_table).load_only(*only_cols))

        if order_by:
            if not isinstance(order_by, list):
                order_by = [order_by]
            for by in order_by:
                self.q = self.q.order_by(by)

        self.fetch_size = fetch_by

        self.row_count = self.q.count()
        self.current_count = min(self.fetch_size, self.row_count)
        self.data = self.q.slice(0, self.current_count).all()
        self.columns = []
        cd = self.q.column_descriptions
        if len(self.q.column_descriptions) > 1:
            # если запрос содержит join
            for col_des in self.q.column_descriptions:
                if hasattr(col_des['expr'], '__table__'):
                    self.columns.extend(
                        map(lambda x: "{0}.{1}".format(col_des['name'], x), col_des['expr'].__table__.columns.keys()))
                else:
                    self.columns.append(col_des['name'])
        else:
            self.columns.extend(cd[0]['expr'].__table__.columns.keys())

        if cols:
            self.columns = [x for x in cols if x in self.columns]

        if header:
            self.header = list(itertools.starmap(lambda x,y: x if x else y, itertools.zip_longest(header, self.columns, fillvalue=None)))
        else:
            self.header = self.columns