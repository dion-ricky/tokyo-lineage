from distutils.core import setup
from setuptools import find_namespace_packages

VERSION = '0.4a3'

setup(
    name="tokyo-lineage",
    packages=find_namespace_packages(include=['tokyo_lineage.*']),
    version=VERSION,
    license="MIT",
    description="Tokyo Lineage",
    author="Dion Ricky Saputra",
    author_email="code@dionricky.com",
    url="https://github.com/dion-ricky/tokyo-lineage",
    keywords=["data lineage"],
    install_requires=[
        "alembic==1.4.2",
        "apache-airflow==1.10.12",
        "apispec==1.3.3",
        "argcomplete==1.12.0",
        "attrs==19.3.0",
        "avro==1.10.0",
        "Babel==2.8.0",
        "cached-property==1.5.1",
        "cachetools==4.2.4",
        "cattrs==1.0.0",
        "certifi==2020.6.20",
        "chardet==3.0.4",
        "click==6.7",
        "colorama==0.4.3",
        "colorlog==4.0.2",
        "configparser==3.5.3",
        "croniter==0.3.34",
        "defusedxml==0.6.0",
        "dill==0.3.2",
        "dnspython==1.16.0",
        "docutils==0.16",
        "email-validator==1.1.1",
        "Flask==1.1.2",
        "Flask-Admin==1.5.4",
        "Flask-AppBuilder==2.3.4",
        "Flask-Babel==1.0.0",
        "Flask-Caching==1.3.3",
        "Flask-JWT-Extended==3.24.1",
        "Flask-Login==0.4.1",
        "Flask-OpenID==1.2.5",
        "Flask-SQLAlchemy==2.4.4",
        "flask-swagger==0.2.14",
        "Flask-WTF==0.14.3",
        "funcsigs==1.0.2",
        "future==0.18.2",
        "google-api-core==2.7.1",
        "google-api-python-client==2.40.0",
        "google-auth==2.6.0",
        "google-auth-httplib2==0.1.0",
        "google-auth-oauthlib==0.5.0",
        "google-cloud-bigquery==2.34.2",
        "google-cloud-bigquery-storage==2.13.0",
        "google-cloud-core==2.2.3",
        "google-crc32c==1.3.0",
        "google-resumable-media==2.3.2",
        "googleapis-common-protos==1.55.0",
        "graphviz==0.14.1",
        "grpcio==1.44.0",
        "grpcio-status==1.44.0",
        "gunicorn==20.0.4",
        "httplib2==0.20.4",
        "idna==2.10",
        "importlib-metadata==1.7.0",
        "iso8601==0.1.12",
        "itsdangerous==1.1.0",
        "Jinja2==2.11.2",
        "json-merge-patch==0.2",
        "jsonschema==3.2.0",
        "lazy-object-proxy==1.5.1",
        "lockfile==0.12.2",
        "Mako==1.1.3",
        "Markdown==2.6.11",
        "MarkupSafe==1.1.1",
        "marshmallow==2.21.0",
        "marshmallow-enum==1.5.1",
        "marshmallow-sqlalchemy==0.23.1",
        "natsort==7.0.1",
        "numpy==1.19.1",
        "oauthlib==3.2.0",
        "openlineage-airflow==0.5.1",
        "openlineage-integration-common==0.5.1",
        "openlineage-python==0.5.1",
        "packaging==21.3",
        "pandas==1.1.0",
        "pandas-gbq==0.14.1",
        "pendulum==1.4.4",
        "prison==0.1.3",
        "proto-plus==1.20.3",
        "protobuf==3.19.4",
        "psutil==5.7.2",
        "pyarrow==6.0.1",
        "pyasn1==0.4.8",
        "pyasn1-modules==0.2.8",
        "pydata-google-auth==1.3.0",
        "Pygments==2.6.1",
        "PyJWT==1.7.1",
        "pyparsing==3.0.7",
        "pyrsistent==0.16.0",
        "python-daemon==2.2.4",
        "python-dateutil==2.8.1",
        "python-editor==1.0.4",
        "python-nvd3==0.15.0",
        "python-slugify==4.0.1",
        "python-snappy==0.5.1",
        "python3-openid==3.2.0",
        "pytz==2020.1",
        "pytzdata==2020.1",
        "PyYAML==5.3.1",
        "requests==2.24.0",
        "requests-oauthlib==1.3.1",
        "rsa==4.8",
        "setproctitle==1.1.10",
        "six==1.15.0",
        "SQLAlchemy==1.3.19",
        "SQLAlchemy-JSONField==0.9.0",
        "SQLAlchemy-Utils==0.36.8",
        "tabulate==0.8.7",
        "tenacity==4.12.0",
        "text-unidecode==1.3",
        "thrift==0.13.0",
        "typing==3.7.4.3",
        "typing-extensions==3.7.4.2",
        "tzlocal==1.5.1",
        "unicodecsv==0.14.1",
        "uritemplate==4.1.1",
        "urllib3==1.25.10",
        "Werkzeug==0.16.1",
        "WTForms==2.3.3",
        "zipp==3.1.0",
        "zope.deprecation==4.4.0"
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ]
)