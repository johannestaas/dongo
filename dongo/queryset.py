'''
dongo
==========

A Django-ORM inspired Mongo ODM.
'''
import six
from .exceptions import DongoResultError

__all__ = ('QuerySet',)


class QuerySet(object):
    '''
    This is the result from calling a ``DongoCollection``'s ``filter``
    classmethod.

    You will only ever need to pull these by calling ``filter``::

        sel = MyDongoCollection.filter(some_field="value")
        for item in sel:
            print(item)

        sel.update(some_field="some new value")

    To fetch all records with x=5 and foo='bar'::

        MyClass.filter(x=5, foo='bar')

    To fetch all records where inside the "accounts" dictionary there's a
    "creditcard" property with value 1000::

        MyClass.filter(accounts__creditcard=1000)

    To fetch all records with x is greater or equal to 5::

        MyClass.filter(x__gte=5)

    To fetch all records where inside the "accounts" dictionary there's a
    "creditcard" property with value greater than 10000::

        MyClass.filter(accounts__creditcard__gt=10000)

    To fetch all records that have color property either red or blue::

        MyClass.filter(color__in=['red', 'blue'])

    To fetch all records where favorite color ISN'T blue::

        MyClass.filter(color__ne='blue')

    To fetch all records where favorite color isn't red or blue, where age
    is greater than 50, and where age is less than or equal to 60::

        MyClass.filter(color__nin=['red', 'blue'], age__gt=50, age__lte=60)
    '''

    def __init__(self, klass, query):
        self.klass = klass
        self.query, self.opts = klass._build_query(query)

    @classmethod
    def _filter_or(cls, klass):
        ms = cls(klass, {})
        ms.query = {'$or': []}
        return ms

    @classmethod
    def _filter_and(cls, klass):
        ms = cls(klass, {})
        ms.query = {'$and': []}
        return ms

    def __iter__(self):
        '''
        Iterate across all objects filtered.

        :yield: new class instance of record
        '''
        return self.iter(timeout=True)

    def _fix_sort_arg(self, kwargs):
        if 'sort' not in kwargs:
            return
        if isinstance(kwargs['sort'], six.string_types):
            kwargs['sort'] = [(kwargs['sort'], 1)]

    def __add__(self, other):
        if '$or' in self.query:
            op = '$or'
        elif '$and' in self.query:
            op = '$and'
        else:
            raise ValueError('top level must be $and or $or')
        self.query[op].append(other.query)
        return self

    def __or__(self, other):
        '''
        Add two query results together::

            for person in (
                PersonCollection.filter(age__gte=21) |
                PersonCollection.filter(birthday__lte=datetime(1997, 1, 1))
            ):
                print(person)

        :param other: the other filter
        :yield: each resulting class instance
        '''
        return QuerySet(self.klass, {'$or': [self.query, other.query]})

    def __and__(self, other):
        '''
        Find intersection between two query results::

            for person in (
                PersonCollection.filter(citizen='us') &
                PersonCollection.filter(age__gte=18)
            ):
                print('{} can vote'.format(person['name']))

        :param other: the other queryset
        :yield: each resulting class instance
        '''
        return QuerySet(self.klass, {'$and': [self.query, other.query]})

    def update(self, **updates):
        '''
        Run database updates to all filtered items::

            Person.filter(age=21).update(drinking=True)

        :param **updates: each keyword update and value
        :return: the pymongo update_many result
        '''
        return self.klass.coll.update_many(self.query, {'$set': updates})

    def inc(self, **updates):
        '''
        Run database increments to all filtered items::

            # Age each person by a year
            Person.filter().update(age=1)

        :param **updates: each field to increment and amount
        :return: the pymongo update_many result
        '''
        return self.klass.coll.update_many(self.query, {'$inc': updates})

    def delete(self, **kwargs):
        '''
        Delete all documents that match the queryset::

            # remove all accounts that haven't been active since 1/1/2000
            Account.filter(last_active__lte=datetime(2000, 1, 1).delete()

        :param **kwargs: keyword arguments to pass to pymongo's ``delete_many``
        :return: the pymongo delete_many result
        '''
        return self.klass.coll.delete_many(self.query, **kwargs)

    def first(self, **kwargs):
        '''
        Fetches the first matching filter or None.

        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param **kwargs: the keywords to pass to pymongo's ``find_one``
        :return: the class instance or None
        '''
        self._fix_sort_arg(kwargs)
        data = self.klass.coll.find_one(self.query, **kwargs)
        if not data:
            return None
        return self.klass(data)

    def first_or_die(self, **kwargs):
        '''
        Fetches the first matching filter or raises ``DongoResultError``.

        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param **kwargs: the keywords to pass to pymongo's ``find_one``
        :return: the class instance
        :raises: ``DongoResultError``
        '''
        data = self.first(**kwargs)
        if not data:
            raise DongoResultError('item not found with query')
        return data

    def iter(self, timeout=True, **kwargs):
        '''
        Performs a query from the keyword arguments.
        You can do the same by just iterating across the ``.filter(...)``
        query, except ``iter`` allows you to pass keywords like
        ``timeout``, ``limit``, and ``sort``.

        :param timeout: whether the query should timeout, default ``True``
        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo
        :yield: each class instance from the query
        '''
        kwargs['no_cursor_timeout'] = not timeout
        self._fix_sort_arg(kwargs)
        return (
            self.klass(d) for d in self.klass.coll.find(self.query, **kwargs)
        )

    def list(self, timeout=True, **kwargs):
        '''
        Returns a list of results rather than an iterator::

            persons = Person.filter(age=21).list()

        :param timeout: whether the query should timeout, default ``True``
        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo
        :return: a list of the instances from the query results
        '''
        return list(self.iter(timeout=timeout, **kwargs))

    def count(self, **kwargs):
        '''
        Like ``list`` but performs a count.

            Person.filter(color='red').count()
            # 10

        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo
        :return: the number of results it found
        '''
        return self.klass.coll.count(self.query, **kwargs)

    def map(self, term, **kwargs):
        '''
        Takes a term and a set of keyword argument query and returns a
        dictionary keyed by the term values, pointing to a list of records where
        its term is that value.

        For example, if there are three Person records and favorite color is
        red for two and blue for the last::

            Person.filter(age__gt=25).map('color')
            # {
            #     'red': [Person(name=Joe, age=26), Person(name=Jack, age=30)],
            #     'blue': [Person(name=Schmoe, age=42)],
            # }

        :param timeout: whether the query should timeout, default ``True``
        :param sort: a field to sort results by initially, either a string or
                     list of (string, direction) tuples like [('age', -1)]
        :param limit: whether to limit records to a certain number
        :param **kwargs: other keywords to pass to pymongo's ``find``
        :return: a dictionary of results ``{term_value: [result1, ...]}``
        '''
        curs = self.list(**kwargs)
        mapped = {}
        for result in curs:
            val = result[term]
            if val not in mapped:
                mapped[val] = []
            mapped[val].append(result)
        return mapped
