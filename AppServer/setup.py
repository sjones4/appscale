from setuptools import setup, find_packages


setup(
    name='appscale-appserver',
    version='0.1.0',
    description='An implementation of the Google AppEngine Python AppServer',
    author='AppScale Systems, Inc.',
    url='https://github.com/AppScale/appscale',
    license='Apache License 2.0',
    keywords='appscale google-app-engine python',
    platforms='Posix',
    install_requires=[
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
    ],
    zip_safe=False,
    namespace_packages=['google'],
    packages=find_packages(include=['google', 'google.*']),
)

