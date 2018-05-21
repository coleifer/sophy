from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True


if cython_installed:
    python_source = 'sophy.pyx'
else:
    python_source = 'sophy.c'
    cythonize = lambda obj: obj

library_source = 'src/sophia.c'

sophy = Extension(
    'sophy',
    #extra_compile_args=['-g', '-O0'],
    #extra_link_args=['-g'],
    sources=[python_source, library_source])

setup(
    name='sophy',
    version='0.6.1',
    description='Python bindings for the sophia database.',
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize([sophy]),
)
