from setuptools import setup

setup(
    name="dbt-remote",
    version='0.1',
    py_modules=['cli'],
    install_requires=[
        'google-api-core',
        'google-cloud-bigquery',
        'google-cloud-tasks',
        'google-cloud-run',
        'google-cloud-core',
        'googleapis-common-protos',
        'Click',
        'requests',
        'dbt-bigquery',
        'dbt-core'
    ],
    entry_points='''
        [console_scripts]
        dbt-remote=cli:cli
    ''',
)
