dongo
=====

A Django-ORM inspired Mongo ODM.

(Requires at least MongoDB 2.6, as PyMongo does)

Installation
------------

From PyPI::

    $ pip install dongo

From the project root directory::

    $ python setup.py install

Usage
-----

Dongo is a Django-ORM inspired ODM for mongodb.

Here are a few examples of the query and class syntax.

Setup
-----

You will need to first connect to a database and host. By default, localhost
port 27017 will be selected, but you will still need to specify the default
database::

    from dongo import connect

    # For the mydatabase named database on localhost
    connect('mydatabase')

    # For your mongodb in the private network
    connect('mydatabase', host='192.168.1.200')

    # For multiple hosts in a replica set
    connect('mydatabase', hosts=['10.0.0.100', '10.0.0.101', '10.0.0.102:27018'], replica_set='myrepset0')

    # A uri can explicitly be specified as well
    connect('mydatabase', uri='mongodb://localhost:27017/')


You can separate collections into different databases, but those connections select
the default database that collections will use if database is unspecified.

Next you will need to declare some sort of collection classes::

    from dongo import DongoCollection
    from datetime import datetime

    class MusicArtist(DongoCollection):
        # if a specific database other than the default is desired, uncomment this:
        # database = 'myotherdatabase'
        collection = 'music_artists'

    # That is all you need to query and read records from the collection "music_artists",
    # and the following would create new records and insert them and query for them.

    queen = MusicArtist({
        'name': 'queen',
        'lead': 'freddie',
        'songs': ['we are the champions', 'we will rock you'],
        'fans': ['jack', 'jill'],
        'nested': {
            'field1': 1,
            'field2': 2,
        },
    })
    # insert must be called manually
    queen.insert()

    # you can use keywords and auto-insert with the "new" classmethod.
    queen_stoneage = MusicArtist.new(
        name='queens of the stone age',
        lead='josh',
        songs=['go with the flow', 'little sister'],
        start=datetime(year=1996, month=1, day=1),
        fans=['jack'],
        nested={
            'field1': 1,
            'field2': 222,
        },
    )

    # queries are simple
    for ma in MusicArtist.filter(fans='jack'):
        print('jack likes ' + ma['name'])

    # you can even do regex queries and bulk updates
    MusicArtist.filter(name__regex='^queen').update(new_field='this is a new field')

    # There are many operators, like __gt, __gte, __lt, __lte, __in, __nin, all corresponding to mongo's
    # operators like $gt.

    # you can do set logic as well with operators: |, &, ~
    # for example less than comparisons and checking field existence:
    for ma in (MusicArtist.filter(start__lt=datetime(2000, 1, 1)) | MusicArtist.filter(start__exists=0)):
        print('either this music artist started before the year 2000 or their startdate is unknown: ' + ma['name'])

    # And you can query inside nested dictionaries

    for ma in MusicArtist.filter(nested__field1=1):
        print(ma)

    # updating the database or fetching fields is as easy as dictionary access
    ma = MusicArtist.filter(name='queen').first()
    ma['new_field'] = 'new_value'
    print(ma['name'])
    ma.set(new_field_2='a', new_field_3='b', new_field_4={'foo': 'bar'})
    ma['nested.field1'] = 'new value in nested field'
    ma.set(nested__field1='reset that nested field to this value')


You will likely want methods associated with records, and to do that you just extend your
class definition::

    class Person(DongoCollection):
        collection = 'persons'

        def print_name(self):
            print(self.get('name', 'unknown'))

        def serialize(self):
            return {
                'name': self.get('name'),
                'age': self.get('age', 0),
                'birthday': self.get('start', datetime.min).isoformat(),
                'favorite_color': self.get('color'),
            }

        def change_color(self, new_color):
            # updates record in database as well
            self['color'] = new_color

        @classmethod
        def start_new_year(cls):
            # add 1 to all age values for every record with a field "age"
            cls.filter(age__exists=1).inc(age=1)
            # kill off those 110 and older
            cls.filter(age__gte=110).delete()

        @classmethod
        def startswith(cls, prefix):
            # find all persons with a name that starts with ``prefix``
            regex = '^{}'.format(prefix)
            return cls.filter(name__regex=regex)

        @classmethod
        def endswith(cls, suffix):
            # find all persons with a name that ends with ``suffix``
            regex = '{}$'.format(suffix)
            return cls.filter(name__regex=regex)

        @classmethod
        def first_10(cls):
            return cls.filter().iter(limit=10, sort='name')

        @classmethod
        def sort_by_oldest_first_then_alphabetically(cls):
            return cls.filter().iter(sort=[('age', -1), ('name', 1)])



Release Notes
-------------

:0.2.3:
    Removed unnecessary dependency
:0.2.2:
    Released alpha with python 2.7 and 3.x compatibility
:0.2.1:
    Released alpha with python 3.x compatibility
