
TODO:
=====

* Add gnureadline to python2.7 
* Best in bootstrap?
* According to https://pypi.python.org/pypi/gnureadline, best to install with pip

Tried the following, but ``setuptools`` dependency was not found :: 
  
   yumdownloader --resolve --enablerepo epel python-pip
   /opt/python/bin/pip2.7 install --upgrade pip
   /opt/python/bin/pip2.7 install gnureadline