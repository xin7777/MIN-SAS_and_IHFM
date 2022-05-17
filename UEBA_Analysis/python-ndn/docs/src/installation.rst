============
Installation
============

Install the latest release with pip::

    $ pip install python-ndn

Install the latest development version::

    $ pip install -U git+https://github.com/named-data/python-ndn.git

Instructions for developer
--------------------------

Setup python3.6 virtual environment with editable installation:

.. code-block:: bash

    python3.6 -m venv venv
    . venv/bin/activate
    pip3 install -e .[dev]

Run all tests:

.. code-block:: bash

    make test

Run static analysis:

.. code-block:: bash

    make lint

Please use python 3.7 or 3.8 to generate the documentation.

.. code-block:: bash

    pip install Sphinx sphinx-autodoc-typehints readthedocs-sphinx-ext \
        sphinx-rtd-theme pycryptodomex pygtrie

    cd docs && make html
    open _build/html/index.html
