#! /bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

#set -e

version=$1
destination=$2

git clone https://github.com/gregneagle/relocatable-python.git

echo "Generating python ${version} into ${destination}"

os_version="10.9"
if [[ "${version}" == "3.10.10" || "${version}" == "3.11.2" ]]; then
    os_version="11"
fi

relocatable-python/make_relocatable_python_framework.py --python-version "${version}" --destination "${destination}" --upgrade-pip --os-version "${os_version}"

rm -rf relocatable-python
