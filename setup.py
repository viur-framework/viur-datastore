from setuptools import setup, Extension
from Cython.Build import cythonize
from Cython.Distutils import build_ext

setup(
	name='viur-dbaccelerator',
	packages=['viur'],
	python_requires=">=3.6",
	cmdclass={'build_ext': build_ext},
	ext_modules=cythonize(Extension("viur.dbaccelerator", ["viur/dbaccelerator.pyx"])),
)
