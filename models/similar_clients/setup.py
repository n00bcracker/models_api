from setuptools import setup, Extension
from Cython.Build import cythonize


extensions = [Extension("cython_funcs", ['cython_funcs.pyx', ])]
setup(
    ext_modules = cythonize(extensions, language_level = "3")
)
