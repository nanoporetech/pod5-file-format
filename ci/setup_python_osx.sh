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
if [[ "${version}" == "3.10.10" || "${version}" == "3.11.2" || "${version}" == "3.12.0" ]]; then
    os_version="11"
fi

# relocatable-python doesn't like this dir existing, it exits with error:
tmp_python_dir="/Users/cirunner/Library/Python_"
function cleanup {
  mv $tmp_python_dir /Users/cirunner/Library/Python
}
trap cleanup EXIT

mv /Users/cirunner/Library/Python $tmp_python_dir

relocatable-python/make_relocatable_python_framework.py --python-version "${version}" --destination "${destination}" --upgrade-pip --os-version "${os_version}"

rm -rf relocatable-python
