# Copyright 2021 christophe
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
import re


YEAR_PATERN =  "(?<year>(\d{2}|\d{4}))"

PREFIXES = {"Q": timedelta(days=91), "M": timedelta(days=30)}


def get_nb_format_pattern(prefixes):
    prefix_matcher = "|".join(prefixes)
    return  f"(?<prefix>:{prefix_matcher})(?P<index>\d+)"


def read_date(value, reference_date=None):
    if isinstance(value, datetime):
        return value
    else:
        if reference_date is None:
            raise ValueError("Cannot provide a date without a reference")
        pattern = get_nb_format_pattern(PREFIXES)
        match = re.findall(pattern, value)[0]
        prefix = match["prefix"]
        index = match["index"]
        return reference_date + index * PREFIXES[prefix]
        

