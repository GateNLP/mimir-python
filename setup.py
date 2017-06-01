from distutils.core import setup

setup(	name='mimir-python',
		version='1.0',
		description='Mimir client library for Python',
		author='Dominic Rout',
		author_email='dom.rout@gmail.com',
		url='https://github.com/GateNLP/mimir-python',
		packages=['mimir'],
		install_requires=[
			'requests'
			]
	 )
