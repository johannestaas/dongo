'''
dongo
==========

A Django-ORM inspired Mongo ODM.
'''


class DongoError(Exception):
    pass


class DongoConnectError(DongoError):
    pass


class DongoResultError(DongoError):
    pass


class DongoCollectionError(DongoError):
    pass
