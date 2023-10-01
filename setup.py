""" this is the setup.py file"""

from typing import List
from setuptools import find_packages, setup


HYPEN_E_DOT = '-e .'


def get_requirements(file_path: str) -> List[str]:
    '''
    this function will return the list of requirements
    '''
    requirements = []
    with open(file_path, encoding='utf-8') as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n", "") for req in requirements]

        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
    return requirements


setup(
    name='turnover',
    version='0.0.1',
    author='Hamza',
    author_email='hamza.landolsi@essai.ucar.tn',
    packages=find_packages(),
    install_requires=get_requirements('requirements.txt')
)
