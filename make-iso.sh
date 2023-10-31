#!/bin/bash
set -e
vagrant up
ansible-playbook -i inventory playbook.yml
#vagrant halt
#disk_path=$(VBoxManage showvminfo $(cat .vagrant/machines/default/virtualbox/id) --machinereadable | grep vmdk | sed 's/.*=//g;s/"//g')
#sudo qemu-nbd -c /dev/nbd0 "${disk_path}"
#sudo mount /dev/nbd0p1 mnt
#
#sudo umount mnt
#sudo qemu-nbd -d /dev/nbd0
