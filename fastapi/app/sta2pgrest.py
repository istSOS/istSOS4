# This tools convert STA requests to postgREST requests.

def stqa2pgr(sta_obj,sta_par):
    pgr_obj = {
        'select': None,
        'limit': None,
        'properties': None,
        'order': None
    }

    if sta_par.select:
        for obj in select:
            pgr_obj['select'][obj]: None
    if sta_par.top:
        pgr_obj['limit'] = sta_par.top

    if sta_par.filter:
        

    sta_par = {
        self.expand = expand,
        self.select = select,
        self.orderby = orderby,
        self.top = top,
        self.skip = skip,
        self.count = count,
        self.filter = filter,
        self.resultFormat = resultFormat
        self.asofsystime = as_of_system_time
    }
