from pydantic import BaseModel, Extra
from fastapi import Query as fQuery
from pypika import Query, Table, Field, Order, Schema, Tables
from datetime import timedelta, datetime

class QueryParameters():
    def __init__(
        self,
        expand: str | None = fQuery(
            default=None,
            title='expand',
            description='comma separated list of sub-entity names or sub-entity names separated by forward slash',
        ),
        select: str | None = fQuery(
            default=None,
            title='select',
            description='comma separated list of property names (including navigation property names)',
        ),
        orderby: str | None = fQuery(
            default=None,
            title='order by',
            description='comma separated list of property names with suffix asc for ascending or desc for descending'
        ),
        top: int | None = fQuery(
            default=None,
            title='top',
            description='Specifies a non-negative integer that limits the number of entities returned'
        ),
        skip: int | None = fQuery(
            default=None,
            title='skip',
            description='Specifies a non-negative integer that limits the number of entities returned'
        ),
        count: bool | None = fQuery(
            default=None,
            title='count',
            description='Is used to retrieve the total number of items in a collection matching the requested entity'
        ),
        filter: str | None = fQuery(
            default=None,
            title='filter',
            description='query option to perform conditional operations on the property values and filter request result'
        ),
        resultFormat: str | None = fQuery(
            default=None,
            title='result format',
            description='return Observations in a data array format'
        ),
        as_of_system_time: datetime | None = fQuery(
            default=None,
            title='as of system time',
            description='return Observations as they were stored in a specific time instant'
        ),
    ):
        self.expand = expand,
        self.select = select,
        self.orderby = orderby,
        self.top = top,
        self.skip = skip,
        self.count = count,
        self.filter = filter,
        self.resultFormat = resultFormat
        self.asofsystime = as_of_system_time
    
    # https://github.com/kayak/pypika
    # https://stackoverflow.com/questions/13227142/using-row-to-json-with-nested-joins
    def to_sql(self, element):
        t_element = Table(element)
        q = Query.from_(t_element)
        if self.select:
            q = q.select(*self.select)
        if self.count:
            q = q.functions.Count(self.count)
        if self.filter:
            q = q.where(*self.filter)
        if self.orderby:
            # -> split and iteratively add order by clause
            oby = self.orderby.split(',')
            for o in oby:
                field, mode = o.split()
                if mode == 'desc':
                    q = q.orderby(field, order=Order.desc)
                else:
                    q = q.orderby(field, order=Order.asc)
        if self.top:
            # -> add LIMIT clause
            q = q.limit(self.top)
        if self.skip:
            q = q.offset(self.skip)
        
        return q.get_sql()




            # .where(customers.age >= 18) \
            # .groupby(customers.id) \
            # .select(customers.id, fn.Sum(customers.revenue))

            # sql_base = f"SLEECT * FROM sensor"
            # sql_where = f"{select}"




            # history, customers = Tables('history', 'customers')
            # q = Query \
            #     .from_(history) \
            #     .join(customers) \
            #     .on(history.customer_id == customers.id) \
            #     .select(history.star) \
            #     .where(customers.id == 5)

                       

#  SEE: https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/

# from fastapi import FastAPI, Depends, Query

# app = FastAPI()


# class SearchArgs:
#     def __init__(
#         self,
#         query: str = Query(...),
#         limit: int = Query(10),
#         offset: int = Query(0),
#         sort: str = Query("date"),
#     ):
#         self.query = query
#         self.limit = limit
#         self.offset = offset
#         self.sort = sort


# @app.get("/api/v1/search_dataclass", tags=["basic"])
# def search(args: SearchArgs = Depends()):
#     return {"detail": "search-result", "args": args, "results": {"abc": "def"}}