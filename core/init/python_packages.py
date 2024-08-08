import pkg_resources

# Get a list of installed Python distributions
distributions = [dist for dist in pkg_resources.working_set]

# Filter out system packages
user_distributions = [dist for dist in distributions if dist.location.endswith('site-packages')]

# Print the module names and versions
for dist in user_distributions:
    print(dist.key, dist.version)


# conda install -c conda-forge pymysql
# conda install -c conda-forge sshtunnel
# conda install -c conda-forge trino-python-client
# conda install -c conda-forge turbodbc
# conda install -c conda-forge mysql-connector-python
# conda install -c conda-forge pandas
# conda install -c conda-forge nbconvert
# conda install -c conda-forge databricks-sql-connector
# conda install -c conda-forge sqlalchemy
# conda install -c conda-forge pyspark
# conda install -c conda-forge databricks-cli
# conda install -c conda-forge cython matplotlib numpy scipy sympy
# conda install -c conda-forge jupyter


# ## package, version
# ## -------------------------------------------
# mako 1.2.4
# sqlalchemy 1.4.47
# alembic 1.10.3
# databricks-sql-connector 2.5.0
# lz4 4.3.2
# mysql-connector 2.2.9
# mysql-connector-python 8.0.31
# oauthlib 3.2.2
# openpyxl 3.1.2
# protobuf 3.20.1
# thrift 0.16.0
# trino 0.314.0
# babel 2.12.1
# cython 0.29.34
# jinja2 3.1.2
# markupsafe 2.1.2
# pillow 9.5.0
# pyjwt 2.7.0
# pymysql 1.0.3
# pynacl 1.5.0
# pyqt5 5.15.7
# pyqt5-sip 12.11.0
# pyqtwebengine 5.15.4
# pysocks 1.7.1
# pyyaml 6.0
# pygments 2.15.1
# qdarkstyle 3.1
# qtawesome 1.2.3
# qtpy 2.3.1
# rtree 1.0.1
# send2trash 1.8.0
# unidecode 1.3.6
# alabaster 0.7.13
# anyio 3.6.2
# argon2-cffi 21.3.0
# argon2-cffi-bindings 21.2.0
# arrow 1.2.3
# astroid 2.15.4
# asttokens 2.2.1
# atomicwrites 1.4.1
# attrs 22.2.0
# autopep8 2.0.2
# backcall 0.2.0
# backports.functools-lru-cache 1.6.4
# bcrypt 3.2.2
# beautifulsoup4 4.12.2
# binaryornot 0.4.4
# black 23.3.0
# bleach 6.0.0
# blinker 1.6.2
# brotlipy 0.7.0
# certifi 2023.5.7
# cffi 1.15.1
# chardet 5.1.0
# charset-normalizer 3.1.0
# click 8.1.3
# cloudpickle 2.2.1
# colorama 0.4.6
# comm 0.1.3
# configparser 5.3.0
# contourpy 1.0.7
# cookiecutter 2.1.1
# cryptography 40.0.2
# cycler 0.11.0
# databricks-cli 0.17.7
# debugpy 1.6.7
# decorator 5.1.1
# defusedxml 0.7.1
# diff-match-patch 20200713
# dill 0.3.6
# dnspython 2.3.0
# docstring-to-markdown 0.12
# docutils 0.19
# entrypoints 0.4
# executing 1.2.0
# fastjsonschema 2.16.3
# flake8 6.0.0
# flit-core 3.8.0
# fonttools 4.39.3
# greenlet 2.0.2
# idna 3.4
# imagesize 1.4.1
# importlib-metadata 6.6.0
# importlib-resources 5.12.0
# inflection 0.5.1
# intervaltree 3.0.2
# ipykernel 6.22.0
# ipython 8.12.0
# ipython-genutils 0.2.0
# ipywidgets 8.0.6
# isort 5.12.0
# jaraco.classes 3.2.3
# jedi 0.18.2
# jellyfish 0.9.0
# jinja2-time 0.2.0
# jsonschema 4.17.3
# jupyter 1.0.0
# jupyter-client 8.2.0
# jupyter-console 6.6.3
# jupyter-core 5.3.0
# jupyter-events 0.6.3
# jupyter-server 2.5.0
# jupyter-server-terminals 0.4.4
# jupyterlab-pygments 0.2.2
# jupyterlab-widgets 3.0.7
# keyring 23.13.1
# kiwisolver 1.4.4
# lazy-object-proxy 1.9.0
# matplotlib 3.7.1
# matplotlib-inline 0.1.6
# mccabe 0.7.0
# mistune 2.0.5
# more-itertools 9.1.0
# mpmath 1.3.0
# munkres 1.1.4
# mypy-extensions 1.0.0
# nbclassic 0.5.5
# nbclient 0.7.4
# nbconvert 7.3.1
# nbformat 5.8.0
# nest-asyncio 1.5.6
# notebook 6.5.4
# notebook-shim 0.2.3
# numpy 1.24.3
# numpydoc 1.5.0
# packaging 23.1
# pandas 2.0.1
# pandocfilters 1.5.0
# paramiko 3.1.0
# parso 0.8.3
# pathspec 0.11.1
# pexpect 4.8.0
# pickleshare 0.7.5
# pip 23.1.1
# pkgutil-resolve-name 1.3.10
# platformdirs 3.3.0
# pluggy 1.0.0
# ply 3.11
# pooch 1.7.0
# prometheus-client 0.16.0
# prompt-toolkit 3.0.38
# psutil 5.9.5
# ptyprocess 0.7.0
# pure-eval 0.2.2
# py4j 0.10.9.7
# pyopenssl 23.1.1
# pyarrow 11.0.0
# pycodestyle 2.10.0
# pycparser 2.21
# pydocstyle 6.3.0
# pyflakes 3.0.1
# pylint 2.17.3
# pylint-venv 3.0.1
# pyls-spyder 0.4.0
# pyparsing 3.0.9
# pyrsistent 0.19.3
# pyspark 3.4.0
# python-dateutil 2.8.2
# python-json-logger 2.0.7
# python-lsp-black 1.2.1
# python-lsp-jsonrpc 1.0.0
# python-lsp-server 1.7.2
# python-slugify 8.0.1
# pytoolconfig 1.2.5
# pytz 2023.3
# pytz-deprecation-shim 0.1.0.post0
# pywin32 304
# pywin32-ctypes 0.2.0
# pywinpty 2.0.10
# pyzmq 25.0.2
# qstylizer 0.2.2
# qtconsole 5.4.2
# requests 2.28.2
# rfc3339-validator 0.1.4
# rfc3986-validator 0.1.1
# rope 1.7.0
# scipy 1.10.1
# setuptools 67.7.2
# sip 6.7.9
# six 1.16.0
# sniffio 1.3.0
# snowballstemmer 2.2.0
# sortedcontainers 2.4.0
# soupsieve 2.3.2.post1
# sphinx 6.2.1
# sphinxcontrib-applehelp 1.0.4
# sphinxcontrib-devhelp 1.0.2
# sphinxcontrib-htmlhelp 2.0.1
# sphinxcontrib-jsmath 1.0.1
# sphinxcontrib-qthelp 1.0.3
# sphinxcontrib-serializinghtml 1.1.5
# spyder 5.4.3
# spyder-kernels 2.4.3
# sshtunnel 0.4.0
# stack-data 0.6.2
# sympy 1.11.1
# tabulate 0.9.0
# terminado 0.15.0
# text-unidecode 1.3
# textdistance 4.5.0
# three-merge 0.1.1
# tinycss2 1.2.1
# toml 0.10.2
# tomli 2.0.1
# tomlkit 0.11.7
# tornado 6.3
# traitlets 5.9.0
# turbodbc 4.5.10
# typing-extensions 4.5.0
# tzdata 2023.3
# tzlocal 4.3
# ujson 5.7.0
# unicodedata2 15.0.0
# urllib3 1.26.15
# watchdog 3.0.0
# wcwidth 0.2.6
# webencodings 0.5.1
# websocket-client 1.5.1
# whatthepatch 1.0.4
# wheel 0.40.0
# widgetsnbextension 4.0.7
# win-inet-pton 1.1.0
# wrapt 1.15.0
# yapf 0.32.0
# zipp 3.15.0