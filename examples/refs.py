from dongo import DongoCollection, connect, deref

connect('dongotest')


class Author(DongoCollection):
    '''
    Example of a Author::

        {
            "name": "joe",
            "age": 20,
        }
    '''
    collection = 'authors'

    @classmethod
    def new(cls, **kwargs):
        kwargs['delete_me'] = True
        return super(Author, cls).new(**kwargs)

    @classmethod
    def cleanup(cls):
        cls.filter(delete_me=True).delete()


class Book(DongoCollection):
    '''
    Example of a book::

        {
            "title": "my book title",
            "authors": [
                {
                    "_dref": ObjectId(...),
                    "_coll": "authors"
                },
                ...
            ]
        }
    '''
    collection = 'books'

    @classmethod
    def new(cls, **kwargs):
        kwargs['delete_me'] = True
        return super(Book, cls).new(**kwargs)

    @classmethod
    def cleanup(cls):
        cls.filter(delete_me=True).delete()


author = Author.new(name='Joe', age=47)
book = Book.new(title="Joe's autobiography", authors=[author.ref()])

author2 = Author.new(name='Phil', age=99)
book2 = Book.new(title="Old Man's autobiography", authors=[author2.ref()])
book3 = Book.new(title="Being Really Old", authors=[author2.ref()])

author3 = Author.new(name='Science Guy', age=120)
book4 = Book.new(title='Physics and Stuff', authors=[author.ref(), author3.ref()])

# Dereference the authors to list(queryset)
science_authors = deref(book4['authors']).list()
print('science authors: {!r}'.format([x['name'] for x in science_authors]))

# Find the book that has a _dref equal to phil's _id (he wrote it)
phil = Author.filter(name='Phil').first()
phils_books = Book.filter(authors___dref=phil['_id']).list()
print('phils books: {!r}'.format([x['title'] for x in phils_books]))


Author.cleanup()
Book.cleanup()
