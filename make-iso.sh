#!/bin/bash
set -e
vagrant up
part_size=$(vagrant ssh -c 'df -h'|grep /dev/sda1|awk '{print $2}')
sudo modprobe nbd max_part=16
disk_path=$(VBoxManage showvminfo $(cat .vagrant/machines/default/virtualbox/id) --machinereadable | grep SATA-0-0 | sed 's/.*=//g;s/"//g')
if [[ "${part_size}" != '46G' ]]
then
  vagrant halt
  sudo qemu-nbd -c /dev/nbd0 "${disk_path}"
  sudo parted /dev/nbd0 resizepart 1 50GB
  sudo qemu-nbd -d /dev/nbd0
  vagrant up
  echo 'Please unlock the user session manually, and then enter Vault password'
fi
say 'Input required' || true
ansible-playbook --ask-vault-password -i inventory playbook.yml
vagrant halt
say 'Input required' || true
sudo qemu-nbd -c /dev/nbd0 "${disk_path}"
sudo mount /dev/nbd0p1 mnt
cp mnt/tmp/andy-liveusb-x86_64.iso ./
sudo umount mnt
sudo qemu-nbd -d /dev/nbd0
