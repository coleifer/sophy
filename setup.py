import os
from setuptools import setup
from setuptools.extension import Extension
try:
    from Cython.Build import cythonize
    cython_installed = True
except ImportError:
    cython_installed = False

if cython_installed:
    python_source = 'sophy.pyx'
else:
    python_source = 'sophy.c'
    cythonize = lambda obj: obj

library_source = os.path.join('src', 'sophia.c')

sophy = Extension(
    'sophy',
    sources=[python_source, library_source])

setup(name='sophy', ext_modules=cythonize([sophy]))
