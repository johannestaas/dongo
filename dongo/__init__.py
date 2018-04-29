'''
dongo

A Django-ORM inspired Mongo ODM.
'''
from .odm import connect, QuerySet, DongoCollection, DongoClient
from .exceptions import (
    DongoError, DongoConnectError, DongoResultError, DongoCollectionError,
)


__title__ = 'dongo'
__version__ = '0.0.1'
__all__ = ('connect', 'QuerySet', 'DongoCollection', 'DongoError',
           'DongoConnectError', 'DongoResultError', 'DongoCollectionError',
           'DongoClient')
__author__ = 'Johan Nestaas <johannestaas@gmail.com>'
__license__ = 'LGPLv3+'
__copyright__ = 'Copyright 2018 Johan Nestaas'
