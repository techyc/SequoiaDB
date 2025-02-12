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

class enum(object):

   def __init__(self, config):
      self.config = config
      for item in config:
         setattr(self, item[1], item[0])

   def choice_tuples(self):
      return ((item[0],item[1]) for item in self.config)

   def available_options(self):
      return (item[0] for item in self.config)
