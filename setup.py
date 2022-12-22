from setuptools import setup, Extension
from Cython.Build import cythonize
from Cython.Distutils import build_ext

setup(
	name='viur-datastore',
	version="1.3.6",
	author="Tobias Steinrücken, Stefan Kögl",
	author_email="devs@viur.dev",
	maintainer="Stefan Kögl",
	maintainer_email="devs@viur.dev",
	description="A faster replacement for google-cloud-datastore",
	long_description=open("README.md", "r").read(),
	long_description_content_type="text/markdown",
	url="https://github.com/viur-framework/viur-datastore",
	packages=['viur.datastore'],
	package_dir={'': 'src'},
	python_requires=">=3.10",
	cmdclass={'build_ext': build_ext},
	ext_modules=cythonize([Extension("viur.datastore.transport", ["src/viur/datastore/transport.pyx"], language="c++", extra_compile_args=["-std=c++11"])]),
	classifiers=[
		"Programming Language :: Python :: 3",
		"Development Status :: 5 - Production/Stable",
		"Intended Audience :: Developers",
		"Topic :: Database :: Front-Ends",
	],
)
