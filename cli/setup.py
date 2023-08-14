from setuptools import setup

setup(
    name="dbt-remote",
    version='0.1',
    py_modules=['cli'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        dbt-remote=cli:cli
    ''',
)
