try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension


try:
    from Cython.Build import cythonize

    extensions = cythonize([
        Extension(
            "sonya.sophia",
            ["sonya/sophia.pyx", "sonya/src/sophia.c"],
        ),
    ], force=True, emit_linenums=False, quiet=True)

except ImportError:
    import warnings
    warnings.warn('Cython not installed, using pre-generated C source file.')
    extensions = [
        Extension(
            "sonya.sophia",
            ["sonya/sophia.c", "sonya/src/sophia.c"],
        ),
    ]

setup(
    name='sonya',
    version='0.4.0',
    description='Python bindings for the sophia database.',
    long_description=open('README.rst').read(),
    author='Charles Leifer',
    author_email="coleifer@gmail.com",
    maintainer='Dmitry Orlov',
    maintainer_email='me@mosquito.su',
    ext_modules=extensions,
    license='BSD',
    include_package_data=True,
    packages=['sonya'],
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    extras_require={
        'develop': [
            'Cython',
            'pytest',
        ],
    },
)
