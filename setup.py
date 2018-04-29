import os
from setuptools import setup

# dongo
# A Django-ORM inspired Mongo ODM, for python 3.x and 2.7


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="dongo",
    version="0.2.3",
    description="A Django-ORM inspired Mongo ODM.",
    author="Johan Nestaas",
    author_email="johannestaas@gmail.com",
    license="LGPLv3+",
    keywords="",
    url="https://github.com/johannestaas/dongo",
    packages=['dongo'],
    package_dir={'dongo': 'dongo'},
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Environment :: Console',
        'Environment :: X11 Applications :: Qt',
        'Environment :: MacOS X',
        'Environment :: Win32 (MS Windows)',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
    ],
    install_requires=['pymongo', 'six'],
    entry_points={
        'console_scripts': [
            'dongo=dongo:main',
        ],
    },
)
