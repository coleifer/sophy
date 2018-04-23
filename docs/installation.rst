.. _installation:

Installing
==========

Up-to-date source code for `sophia <http://sophia.systems>`_ is bundled with
the ``sophy`` source code, so the only thing you need to build is `Cython <http://cython.org>`_.
If Cython is not installed, then the pre-generated C source files will be used.

`sophy <https://github.com/coleifer/sophy>`_ can be installed directly from the
source or from `pypi <https://pypi.python.org/pypi/sophy>`_ using ``pip``.

Installing with pip
-------------------

To install from PyPI:

.. code-block:: bash

    $ pip install cython  # optional
    $ pip install sophy

To install the very latest version, you can install with git:

.. code-block:: bash

    $ pip install -e git+https://github.com/coleifer/sophy#egg=sophy

Obtaining the source code
-------------------------

The source code is hosted on `github <https://github.com/coleifer/sophy>`_ and
can be obtained and installed:

.. code-block:: bash

    $ git clone https://github.com/colefer/sophy
    $ cd sophy
    $ python setup.py build
    $ python setup.py install

Running the tests
-----------------

Unit-tests and integration tests are distributed with the source and can be run
from the root of the checkout:

.. code-block:: bash

    $ python tests.py
