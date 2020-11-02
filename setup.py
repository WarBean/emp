import versioneer
from setuptools import setup, find_packages

setup(
    name='PyEMP',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(),
    zip_safe=False,
    install_requires=['ray'],
    include_package_data=True,
    url='https://github.com/WarBean/emp',
)
