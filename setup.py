
import setuptools

with open('README.md', 'r', encoding='utf-8') as file:
    long_description = file.read()

with open('requirements.txt') as file:
    requirements = [line.strip() for line in file.readlines() if not line.startswith('git+')]

setuptools.setup(
    name='CRIMAC-annotations tools',
    version='0.0.1',
    description='',
    long_description=long_description,
    long_description_content_type='',
    url='',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    packages=setuptools.find_packages(),
    install_requires=requirements,
)
