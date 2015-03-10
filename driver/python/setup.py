#   Copyright (C) 2012-2014 SqlDB Ltd.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from distutils.core import Extension, setup
import sys
import os
import glob
import shutil

if 'win32' == sys.platform:
   dlls = './pysqldb/*.dll'
   for file in glob.glob(dlls):
      if file.startswith('lib'):
         newname = file[3:]
      newname = file[:-3] + 'pyd'
      shutil.copy(file, newname)
   modules = ['err.prop','*.pyd'] #, '*.exp', '*.lib', 
else:
   modules = ['err.prop', '*.so']

extra_opts = {}
extra_opts['packages'] = [ 'bson', 'pysqldb']
extra_opts['package_dir']={ 'pysqldb':'pysqldb', 'bson':'bson'}
extra_opts['package_data'] = { 'pysqldb':modules,
                               'bson':[ 'buffer.h',
                                        'buffer.c',
                                        '_cbsonmodule.h',
                                        '_cbsonmodule.c',
                                        'encoding_helpers.h',
                                        'encoding_helpers.c',
                                        'time64.h',
                                        'time64.c',
                                        'time64_config.h',
                                        'time64_limits.h', ],}
#extra_opts['ext_modules'] = ext_modules
setup(name = 'pysqldb',
      version = '1.0',
      author = 'SqlDB Inc.',
      license = 'GNU Affero GPL',
      description = 'This is a sqldb python driver use adapter package',
      url = 'http://www.sqldb.com',
      **extra_opts)

if 'win32' == sys.platform:
   pyds = './pysqldb/*.pyd'
   for file in glob.glob(pyds):
      os.remove(file)
